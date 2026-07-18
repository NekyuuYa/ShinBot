"""Fail-closed recovery materializers for orphaned session-actor work.

The recovery scanner proves that no live mailbox, effect, route, or external
action authority remains before these materializers run.  That proof does not
make a lost model result reproducible, so the only automatic repair here is to
terminalize the orphaned operation without replaying it and return the session
to an ordinary durable review schedule.  Message-ledger consumption is never
part of a recovery transition.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate
from shinbot.agent.runtime.session_actor.events import (
    ReviewScheduleStatus,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RecoveryCertificate,
    RecoveryDecisionKind,
    RecoveryInvariantSeverity,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryCommitIntent,
    RecoveryMaterializationBlocked,
    RecoveryMaterializer,
)

_RECOVERY_SOURCE = "session_actor_recovery"
_RECOVERY_TRIGGER = "orphaned_operation_recovery"
_RECOVERY_OUTCOME = "defaulted"
_RECOVERY_REASON = "orphaned_operation_settled_without_replay"
_RECOVERY_FAILURE_CODE_PREFIX = "orphaned_operation_recovered"
_RECOVERY_CONTROL_INTENT_KEY = "effect_control_intents"
_RECOVERY_OPERATION_FENCES_KEY = "operation_fences"
_SETTLING_EXIT_CONTROL_KIND = "enqueue_active_chat_exit_request"
_ROUND_DUE_CONTROL_KIND = "enqueue_active_chat_round_due"


@dataclass(slots=True, frozen=True)
class _OrphanedWorkToIdleMaterializer:
    """Settle one proven orphaned operation without recreating external I/O."""

    state: str
    delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        """Validate the only policy input owned by the materializer."""

        if not isinstance(self.state, str) or not self.state.strip():
            raise ValueError("state must be non-empty")
        if isinstance(self.delay_seconds, bool) or not isinstance(
            self.delay_seconds, (int, float)
        ):
            raise TypeError("delay_seconds must be a finite non-negative number")
        delay = float(self.delay_seconds)
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay_seconds must be a finite non-negative number")
        object.__setattr__(self, "delay_seconds", delay)

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition | RecoveryMaterializationBlocked:
        """Create a no-replay fallback transition after certificate revalidation."""

        operation = self._operation(aggregate)
        if isinstance(operation, RecoveryMaterializationBlocked):
            return operation
        operation_id, operation_kind = operation
        blocked = _validate_recovery_authority(
            aggregate=aggregate,
            intent=intent,
            certificate=certificate,
            expected_state=self.state,
            operation_id=operation_id,
            operation_kind=operation_kind,
        )
        if blocked is not None:
            return blocked
        data = _without_terminal_operation_fence(aggregate.data, operation_id)
        if isinstance(data, RecoveryMaterializationBlocked):
            return data
        blocked = self._validate_state_data(
            aggregate=aggregate,
            data=data,
            operation_id=operation_id,
        )
        if blocked is not None:
            return blocked
        return _settle_to_idle(
            aggregate=aggregate,
            operation_id=operation_id,
            operation_kind=operation_kind,
            data=data,
            delay_seconds=self.delay_seconds,
        )

    def _operation(
        self,
        aggregate: AgentSessionAggregate,
    ) -> tuple[str, str] | RecoveryMaterializationBlocked:
        """Return the sole state-owned orphaned operation identity."""

        raise NotImplementedError

    def _validate_state_data(
        self,
        *,
        aggregate: AgentSessionAggregate,
        data: dict[str, Any],
        operation_id: str,
    ) -> RecoveryMaterializationBlocked | None:
        """Check state-specific data before it is discarded by recovery."""

        del aggregate, data, operation_id
        return None


@dataclass(slots=True, frozen=True)
class ReviewRecoveryMaterializer(_OrphanedWorkToIdleMaterializer):
    """Recover an orphaned review by scheduling a fresh ordinary review."""

    state: str = "review"

    def _operation(
        self,
        aggregate: AgentSessionAggregate,
    ) -> tuple[str, str] | RecoveryMaterializationBlocked:
        """Require exactly the aggregate's review operation."""

        operation_id = _exact_text(aggregate.review_operation_id)
        if not operation_id:
            return RecoveryMaterializationBlocked(code="recovery_review_operation_missing")
        return operation_id, "review"


@dataclass(slots=True, frozen=True)
class ActiveReplyRecoveryMaterializer(_OrphanedWorkToIdleMaterializer):
    """Recover an orphaned active reply without replaying the reply model call."""

    state: str = "active_reply"

    def _operation(
        self,
        aggregate: AgentSessionAggregate,
    ) -> tuple[str, str] | RecoveryMaterializationBlocked:
        """Require exactly the aggregate's active-reply operation."""

        operation_id = _exact_text(aggregate.active_reply_operation_id)
        if not operation_id:
            return RecoveryMaterializationBlocked(
                code="recovery_active_reply_operation_missing"
            )
        return operation_id, "active_reply"


@dataclass(slots=True, frozen=True)
class ActiveChatRecoveryMaterializer(_OrphanedWorkToIdleMaterializer):
    """Recover a bootstrap or round by exiting chat without replaying it."""

    state: str = "active_chat"

    def _operation(
        self,
        aggregate: AgentSessionAggregate,
    ) -> tuple[str, str] | RecoveryMaterializationBlocked:
        """Allow one orphaned bootstrap or round, never both at once."""

        round_operation_id = _exact_text(aggregate.active_chat_round_operation_id)
        active_state = aggregate.active_chat_state
        bootstrap_operation_id = _exact_text(active_state.get("bootstrap_operation_id"))
        if round_operation_id and bootstrap_operation_id:
            return RecoveryMaterializationBlocked(
                code="recovery_active_chat_multiple_operations"
            )
        if round_operation_id:
            return round_operation_id, "active_chat_round"
        if bootstrap_operation_id:
            return bootstrap_operation_id, "active_chat_bootstrap"
        return RecoveryMaterializationBlocked(
            code="recovery_active_chat_operation_missing"
        )

    def _validate_state_data(
        self,
        *,
        aggregate: AgentSessionAggregate,
        data: dict[str, Any],
        operation_id: str,
    ) -> RecoveryMaterializationBlocked | None:
        """Accept only the completed timer control that launched a round."""

        active_state = aggregate.active_chat_state
        round_operation_id = _exact_text(aggregate.active_chat_round_operation_id)
        bootstrap_operation_id = _exact_text(active_state.get("bootstrap_operation_id"))
        if round_operation_id:
            if (
                round_operation_id != operation_id
                or bootstrap_operation_id
                or _exact_text(active_state.get("bootstrap_status")) != "completed"
                or _exact_text(active_state.get("round_operation_id")) != operation_id
            ):
                return RecoveryMaterializationBlocked(
                    code="recovery_active_chat_round_shape_invalid"
                )
            control_intents = data.get(_RECOVERY_CONTROL_INTENT_KEY)
            if not isinstance(control_intents, Mapping) or set(control_intents) != {
                _ROUND_DUE_CONTROL_KIND
            }:
                return RecoveryMaterializationBlocked(
                    code="recovery_active_chat_round_control_invalid"
                )
            round_control = control_intents.get(_ROUND_DUE_CONTROL_KIND)
            if not isinstance(round_control, Mapping):
                return RecoveryMaterializationBlocked(
                    code="recovery_active_chat_round_control_invalid"
                )
            if (
                _exact_text(round_control.get("status")) != "completed"
                or _exact_text(round_control.get("effect_kind"))
                != _ROUND_DUE_CONTROL_KIND
                or _exact_text(round_control.get("operation_id"))
                or _exact_text(round_control.get("plan_id"))
                != aggregate.current_plan_id
                or _exact_nonnegative_int(round_control.get("ownership_generation"))
                != aggregate.ownership_generation
                or _exact_nonnegative_int(round_control.get("active_epoch"))
                != aggregate.active_epoch
                or _exact_nonnegative_int(round_control.get("activity_generation"))
                != aggregate.activity_generation
            ):
                return RecoveryMaterializationBlocked(
                    code="recovery_active_chat_round_control_invalid"
                )
            data.pop(_RECOVERY_CONTROL_INTENT_KEY, None)
            return None
        if (
            bootstrap_operation_id != operation_id
            or _exact_text(active_state.get("bootstrap_status")) != "pending"
            or _exact_text(active_state.get("round_operation_id"))
            or data.get(_RECOVERY_CONTROL_INTENT_KEY) not in (None, {}, [])
        ):
            return RecoveryMaterializationBlocked(
                code="recovery_active_chat_bootstrap_shape_invalid"
            )
        return None


@dataclass(slots=True, frozen=True)
class ActiveChatSettlingRecoveryMaterializer(_OrphanedWorkToIdleMaterializer):
    """Settle a lost idle-planning result through the fixed fallback schedule."""

    state: str = "active_chat_settling"

    def _operation(
        self,
        aggregate: AgentSessionAggregate,
    ) -> tuple[str, str] | RecoveryMaterializationBlocked:
        """Require the authoritative idle-planning operation."""

        operation_id = _exact_text(aggregate.idle_planning_operation_id)
        if not operation_id:
            return RecoveryMaterializationBlocked(
                code="recovery_idle_planning_operation_missing"
            )
        return operation_id, "idle_review_planning"

    def _validate_state_data(
        self,
        *,
        aggregate: AgentSessionAggregate,
        data: dict[str, Any],
        operation_id: str,
    ) -> RecoveryMaterializationBlocked | None:
        """Require a complete, settled exit record before bypassing its planner."""

        idle_exit = data.get("idle_exit")
        if not isinstance(idle_exit, Mapping):
            return RecoveryMaterializationBlocked(
                code="recovery_settling_idle_exit_missing"
            )
        if _exact_text(idle_exit.get("operation_id")) != operation_id:
            return RecoveryMaterializationBlocked(
                code="recovery_settling_idle_exit_operation_changed"
            )
        successor_plan_id = _exact_text(idle_exit.get("plan_id"))
        if not successor_plan_id or successor_plan_id == aggregate.current_plan_id:
            return RecoveryMaterializationBlocked(
                code="recovery_settling_successor_plan_invalid"
            )
        if (
            _exact_nonnegative_int(idle_exit.get("ownership_generation"))
            != aggregate.ownership_generation
            or _exact_nonnegative_int(idle_exit.get("active_epoch"))
            != aggregate.active_epoch
            or _exact_nonnegative_int(idle_exit.get("activity_generation"))
            != aggregate.activity_generation
        ):
            return RecoveryMaterializationBlocked(
                code="recovery_settling_idle_exit_fence_changed"
            )
        control_intents = data.get(_RECOVERY_CONTROL_INTENT_KEY)
        if control_intents is None:
            data.pop("idle_exit", None)
            return None
        if not isinstance(control_intents, Mapping):
            return RecoveryMaterializationBlocked(
                code="recovery_settling_control_intents_invalid"
            )
        if set(control_intents) != {_SETTLING_EXIT_CONTROL_KIND}:
            return RecoveryMaterializationBlocked(
                code="recovery_settling_control_intents_unsupported"
            )
        exit_control = control_intents.get(_SETTLING_EXIT_CONTROL_KIND)
        if not isinstance(exit_control, Mapping):
            return RecoveryMaterializationBlocked(
                code="recovery_settling_exit_control_invalid"
            )
        if (
            _exact_text(exit_control.get("status")) != "completed"
            or _exact_text(exit_control.get("effect_kind"))
            != _SETTLING_EXIT_CONTROL_KIND
            or _exact_text(exit_control.get("operation_id"))
            or _exact_text(exit_control.get("plan_id")) != aggregate.current_plan_id
            or _exact_nonnegative_int(exit_control.get("ownership_generation"))
            != aggregate.ownership_generation
            or _exact_nonnegative_int(exit_control.get("active_epoch"))
            != aggregate.active_epoch
            or _exact_nonnegative_int(exit_control.get("activity_generation"))
            != aggregate.activity_generation
        ):
            return RecoveryMaterializationBlocked(
                code="recovery_settling_exit_control_fence_changed"
            )
        data.pop("idle_exit", None)
        data.pop(_RECOVERY_CONTROL_INTENT_KEY, None)
        return None


def builtin_recovery_materializers(
    *,
    delay_seconds: float = 0.0,
) -> dict[str, RecoveryMaterializer]:
    """Return the complete no-replay materializer set for non-idle states."""

    return {
        "review": ReviewRecoveryMaterializer(delay_seconds=delay_seconds),
        "active_reply": ActiveReplyRecoveryMaterializer(delay_seconds=delay_seconds),
        "active_chat": ActiveChatRecoveryMaterializer(delay_seconds=delay_seconds),
        "active_chat_settling": ActiveChatSettlingRecoveryMaterializer(
            delay_seconds=delay_seconds
        ),
    }


def _validate_recovery_authority(
    *,
    aggregate: AgentSessionAggregate,
    intent: RecoveryCommitIntent,
    certificate: RecoveryCertificate,
    expected_state: str,
    operation_id: str,
    operation_kind: str,
) -> RecoveryMaterializationBlocked | None:
    """Require exactly the certificate shape this no-replay policy supports."""

    if aggregate.state != expected_state:
        return RecoveryMaterializationBlocked(code="recovery_materializer_state_changed")
    if certificate.decision.kind is not RecoveryDecisionKind.RECOVER_ORPHANED_WORK:
        return RecoveryMaterializationBlocked(code="recovery_materializer_not_authorized")
    if (
        certificate.subject.profile_id != aggregate.profile_id
        or certificate.subject.session_id != aggregate.session_id
        or certificate.subject.ownership_generation != aggregate.ownership_generation
        or certificate.aggregate_fence.state != expected_state
    ):
        return RecoveryMaterializationBlocked(code="recovery_materializer_subject_changed")
    if certificate.case_identity.case_id != intent.case_id:
        return RecoveryMaterializationBlocked(code="recovery_materializer_case_changed")
    if any(
        invariant.severity is RecoveryInvariantSeverity.BLOCKING
        for invariant in certificate.invariants
    ):
        return RecoveryMaterializationBlocked(
            code="recovery_materializer_certificate_blocked"
        )
    if certificate.decision.target_node_identities != (f"operation:{operation_id}",):
        return RecoveryMaterializationBlocked(
            code="recovery_materializer_orphan_target_changed"
        )
    data = aggregate.data
    if not isinstance(data, Mapping):
        return RecoveryMaterializationBlocked(code="recovery_materializer_data_invalid")
    forbidden = (
        "pending_outbound_actions",
        "outbound_continuation",
        "outbound_blocked",
        "review_cancellation_blocked",
    )
    for field_name in forbidden:
        if data.get(field_name) not in (None, {}, [], ""):
            return RecoveryMaterializationBlocked(
                code=f"recovery_{field_name}_unsupported"
            )
    controls = data.get(_RECOVERY_CONTROL_INTENT_KEY)
    if expected_state not in {"active_chat", "active_chat_settling"} and controls not in (
        None,
        {},
        [],
    ):
        return RecoveryMaterializationBlocked(
            code="recovery_control_intents_unsupported"
        )
    if expected_state != "active_chat_settling" and data.get("idle_exit") not in (
        None,
        {},
        [],
        "",
    ):
        return RecoveryMaterializationBlocked(code="recovery_idle_exit_unsupported")
    operation_fences = data.get(_RECOVERY_OPERATION_FENCES_KEY)
    if not isinstance(operation_fences, Mapping) or set(operation_fences) != {
        operation_id
    }:
        return RecoveryMaterializationBlocked(
            code="recovery_operation_fence_shape_invalid"
        )
    fence = operation_fences.get(operation_id)
    if not isinstance(fence, Mapping):
        return RecoveryMaterializationBlocked(code="recovery_operation_fence_missing")
    if (
        _exact_text(fence.get("operation_id")) != operation_id
        or _exact_text(fence.get("operation_kind")) != operation_kind
        or _exact_nonnegative_int(fence.get("ownership_generation"))
        != aggregate.ownership_generation
        or _exact_nonnegative_int(fence.get("input_watermark")) is None
        or _exact_nonnegative_int(fence.get("input_ledger_sequence")) is None
    ):
        return RecoveryMaterializationBlocked(
            code="recovery_operation_fence_incomplete"
        )
    return None


def _without_terminal_operation_fence(
    data: Mapping[str, Any],
    operation_id: str,
) -> dict[str, Any] | RecoveryMaterializationBlocked:
    """Remove the one proven terminal fence without changing message state."""

    copied = dict(data)
    fences = copied.get(_RECOVERY_OPERATION_FENCES_KEY)
    if not isinstance(fences, Mapping) or set(fences) != {operation_id}:
        return RecoveryMaterializationBlocked(
            code="recovery_operation_fence_shape_invalid"
        )
    copied.pop(_RECOVERY_OPERATION_FENCES_KEY, None)
    return copied


def _settle_to_idle(
    *,
    aggregate: AgentSessionAggregate,
    operation_id: str,
    operation_kind: str,
    data: dict[str, Any],
    delay_seconds: float,
) -> SessionTransition:
    """Build one terminal operation and fresh default review-plan transition."""

    plan_id = _recovery_identifier(
        aggregate,
        operation_id=operation_id,
        purpose="review-plan",
    )
    schedule_event_id = _recovery_identifier(
        aggregate,
        operation_id=operation_id,
        purpose="review-schedule-event",
    )
    plan_revision = aggregate.review_plan_revision + 1
    next_state_revision = aggregate.state_revision + 1
    schedule_outcome = {
        "kind": _RECOVERY_OUTCOME,
        "applied_delay_seconds": delay_seconds,
        "requested_delay_seconds": None,
        "reason": _RECOVERY_REASON,
        "fallback_reason": _RECOVERY_REASON,
        "mention_sensitivity": "normal",
        "active_reply_threshold": {},
        "model_execution_id": "",
        "prompt_signature": "",
        "source": _RECOVERY_SOURCE,
    }
    review_plan = {
        "plan_id": plan_id,
        "plan_revision": plan_revision,
        "trigger": _RECOVERY_TRIGGER,
        **schedule_outcome,
        "expected_active_epoch": aggregate.active_epoch,
        "expected_activity_generation": aggregate.activity_generation,
        "committed_state_revision": next_state_revision,
    }
    target = aggregate.advance(
        state="idle",
        current_plan_id=plan_id,
        review_plan_revision=plan_revision,
        review_plan=review_plan,
        review_operation_id="",
        active_reply_operation_id="",
        active_chat_round_operation_id="",
        idle_planning_operation_id="",
        active_reply_resume={},
        active_chat_state={},
        data=data,
    )
    operation = SessionOperation(
        operation_id=operation_id,
        kind=operation_kind,
        status=SessionOperationStatus.FAILED,
        failure_code=f"{_RECOVERY_FAILURE_CODE_PREFIX}:{operation_kind}",
        failure_message="operation result was unavailable after recovery",
        metadata={
            "recovery": {
                "action": "settled_without_replay",
                "from_state": aggregate.state,
                "pending_ledger": "preserved",
            }
        },
    )
    schedule = SessionReviewSchedule(
        plan_id=plan_id,
        plan_revision=plan_revision,
        applied_delay_seconds=delay_seconds,
        status=ReviewScheduleStatus.SCHEDULED,
        trigger=_RECOVERY_TRIGGER,
        outcome=_RECOVERY_OUTCOME,
        source=_RECOVERY_SOURCE,
        reason=_RECOVERY_REASON,
        fallback_reason=_RECOVERY_REASON,
        expected_active_epoch=aggregate.active_epoch,
        expected_activity_generation=aggregate.activity_generation,
        committed_state_revision=target.state_revision,
    )
    schedule_event = SessionReviewScheduleEvent(
        schedule_event_id=schedule_event_id,
        event_type="scheduled",
        plan_id=plan_id,
        previous_plan_id=aggregate.current_plan_id,
        trigger=_RECOVERY_TRIGGER,
        outcome=_RECOVERY_OUTCOME,
        source=_RECOVERY_SOURCE,
        applied_delay_seconds=delay_seconds,
        reason=_RECOVERY_REASON,
        fallback_reason=_RECOVERY_REASON,
        expected_active_epoch=aggregate.active_epoch,
        expected_activity_generation=aggregate.activity_generation,
        committed_state_revision=target.state_revision,
        operation_id=operation_id,
        metadata={
            "plan_revision": plan_revision,
            "schedule_outcome": schedule_outcome,
        },
    )
    return SessionTransition(
        aggregate=target,
        disposition="recovery_orphaned_operation_settled",
        caused_operation_id=operation_id,
        caused_plan_id=plan_id,
        operations=(operation,),
        review_schedules=(schedule,),
        review_schedule_events=(schedule_event,),
        result={
            "recovery": {
                "action": "settled_to_idle",
                "from_state": aggregate.state,
                "operation_kind": operation_kind,
                "pending_ledger": "preserved",
            },
            "plan_id": plan_id,
        },
        reason=_RECOVERY_REASON,
    )


def _recovery_identifier(
    aggregate: AgentSessionAggregate,
    *,
    operation_id: str,
    purpose: str,
) -> str:
    """Return a deterministic, session-scoped identity for one recovery record."""

    digest = hashlib.sha256(
        "\0".join(
            (
                aggregate.profile_id,
                aggregate.session_id,
                str(aggregate.ownership_generation),
                aggregate.state,
                str(aggregate.state_revision),
                operation_id,
                purpose,
            )
        ).encode("utf-8")
    ).hexdigest()
    return f"recovery-{purpose}:v1:{digest}"


def _exact_text(value: object) -> str:
    """Return canonical non-empty text or an empty string for unsafe input."""

    if not isinstance(value, str) or not value or value != value.strip():
        return ""
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return ""
    return value


def _exact_nonnegative_int(value: object) -> int | None:
    """Return one exact non-negative integer without coercion."""

    if type(value) is not int or value < 0:
        return None
    return value


__all__ = [
    "ActiveChatRecoveryMaterializer",
    "ActiveChatSettlingRecoveryMaterializer",
    "ActiveReplyRecoveryMaterializer",
    "ReviewRecoveryMaterializer",
    "builtin_recovery_materializers",
]
