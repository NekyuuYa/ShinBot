"""Pure workflow adapter for durable idle-review planning effects.

This module deliberately has no dependency on the legacy scheduler or active
chat coordinator. The actor owns the operation fence and effect executor; a
workflow port may only return a relative scheduling proposal.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.idle_review_planning import (
    IdleReviewPlanningInput,
    IdleReviewPlanningInputError,
)
from shinbot.agent.scheduler.models import MentionSensitivity
from shinbot.agent.services.context.review_context_builder import ReviewStageInput

_IDLE_REVIEW_PLANNING_EFFECT_KIND = "run_idle_review_planning"
_MAX_DIAGNOSTIC_TEXT_LENGTH = 512
_LEGACY_BYPASS_CONTRACT_VERSION = 1
_DESCRIPTOR_CONTRACT_VERSION = 2
_ACTOR_NATIVE_CANCELLABLE_CONTRACT_VERSION = 3
_SUPPORTED_EFFECT_CONTRACT_VERSIONS = frozenset(
    {
        _LEGACY_BYPASS_CONTRACT_VERSION,
        _DESCRIPTOR_CONTRACT_VERSION,
        _ACTOR_NATIVE_CANCELLABLE_CONTRACT_VERSION,
    }
)


class IdleReviewPlanningAdapterError(ValueError):
    """Raised when an idle-review planning effect crosses an unsafe boundary."""


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningEffectInput:
    """Trusted actor-owned input for one planner effect invocation."""

    key: SessionKey
    operation_id: str
    plan_id: str
    effect_id: str
    idempotency_key: str
    source_event_id: str
    ownership_generation: int
    active_epoch: int
    activity_generation: int
    input_watermark: int
    input_ledger_sequence: int | None
    trigger: str
    source: str
    planning_input: IdleReviewPlanningInput

    def __post_init__(self) -> None:
        """Validate effect identity and the descriptor bound to its fence."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("key must be a SessionKey")
        for field_name in (
            "operation_id",
            "plan_id",
            "effect_id",
            "idempotency_key",
            "source_event_id",
            "trigger",
            "source",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        for field_name in (
            "ownership_generation",
            "active_epoch",
            "activity_generation",
            "input_watermark",
        ):
            _nonnegative_int(getattr(self, field_name), field_name=field_name)
        if self.ownership_generation < 1:
            raise IdleReviewPlanningAdapterError(
                "ownership_generation must be a positive integer"
            )
        if self.input_ledger_sequence is not None:
            _nonnegative_int(
                self.input_ledger_sequence,
                field_name="input_ledger_sequence",
            )
        if not isinstance(self.planning_input, IdleReviewPlanningInput):
            raise TypeError("planning_input must be an IdleReviewPlanningInput")
        if self.planning_input.input_watermark != self.input_watermark:
            raise IdleReviewPlanningAdapterError(
                "planning_input changed input_watermark"
            )
        if self.planning_input.active_epoch != self.active_epoch:
            raise IdleReviewPlanningAdapterError("planning_input changed active_epoch")
        if self.planning_input.activity_generation != self.activity_generation:
            raise IdleReviewPlanningAdapterError(
                "planning_input changed activity_generation"
            )
        if self.planning_input.trigger != self.trigger:
            raise IdleReviewPlanningAdapterError("planning_input changed trigger")

    @classmethod
    def from_effect_context(
        cls,
        context: EffectExecutionContext,
    ) -> IdleReviewPlanningEffectInput:
        """Decode and cross-check one claimed actor effect."""

        effect = context.effect
        if effect.kind != _IDLE_REVIEW_PLANNING_EFFECT_KIND:
            raise IdleReviewPlanningAdapterError(
                "idle review planning adapter expected "
                f"{_IDLE_REVIEW_PLANNING_EFFECT_KIND!r}, got {effect.kind!r}"
            )
        payload = effect.payload
        if not isinstance(payload, Mapping):
            raise IdleReviewPlanningAdapterError("idle planning effect payload must be an object")
        try:
            planning_input = IdleReviewPlanningInput.from_payload(
                _required_object(payload.get("planning_input"), field_name="planning_input")
            )
        except IdleReviewPlanningInputError as exc:
            raise IdleReviewPlanningAdapterError(str(exc)) from exc
        return cls(
            key=effect.key,
            operation_id=_required_text(effect.operation_id, field_name="operation_id"),
            plan_id=_required_text(payload.get("plan_id"), field_name="plan_id"),
            effect_id=_required_text(effect.effect_id, field_name="effect_id"),
            idempotency_key=_required_text(
                effect.idempotency_key,
                field_name="idempotency_key",
            ),
            source_event_id=_required_text(
                effect.source_event_id,
                field_name="source_event_id",
            ),
            ownership_generation=_positive_int(
                effect.ownership_generation,
                field_name="ownership_generation",
            ),
            active_epoch=_nonnegative_int(
                payload.get("active_epoch"),
                field_name="active_epoch",
            ),
            activity_generation=_nonnegative_int(
                payload.get("activity_generation"),
                field_name="activity_generation",
            ),
            input_watermark=_nonnegative_int(
                payload.get("input_watermark"),
                field_name="input_watermark",
            ),
            input_ledger_sequence=_optional_nonnegative_int(
                payload.get("input_ledger_sequence"),
                field_name="input_ledger_sequence",
            ),
            trigger=_required_text(payload.get("trigger"), field_name="trigger"),
            source=_required_text(payload.get("source"), field_name="source"),
            planning_input=planning_input,
        )


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningWorkflowRequest:
    """One pure planner request derived from a claimed actor effect."""

    effect: IdleReviewPlanningEffectInput

    def __post_init__(self) -> None:
        """Reject requests without the trusted actor effect input."""

        if not isinstance(self.effect, IdleReviewPlanningEffectInput):
            raise TypeError("effect must be an IdleReviewPlanningEffectInput")


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningWorkflowOutput:
    """A relative review proposal returned by a pure planner workflow."""

    next_review_after_seconds: float | None = None
    reason: str = "idle_review_planning_defaulted"
    mention_sensitivity: MentionSensitivity | None = None
    mention_wake_count: int | None = None
    mention_wake_window_seconds: float | None = None
    model_execution_id: str = ""
    prompt_signature: str = ""
    failure_code: str = ""
    failure_message: str = ""

    def __post_init__(self) -> None:
        """Validate model output before it is turned into a completion event."""

        if self.next_review_after_seconds is not None:
            delay = _positive_finite(
                self.next_review_after_seconds,
                field_name="next_review_after_seconds",
            )
            object.__setattr__(self, "next_review_after_seconds", delay)
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, field_name="reason"),
        )
        if self.mention_sensitivity is not None:
            try:
                sensitivity = MentionSensitivity(self.mention_sensitivity)
            except (TypeError, ValueError) as exc:
                raise IdleReviewPlanningAdapterError(
                    "mention_sensitivity is invalid"
                ) from exc
            object.__setattr__(self, "mention_sensitivity", sensitivity)
        count = self.mention_wake_count
        window = self.mention_wake_window_seconds
        if (count is None) != (window is None):
            raise IdleReviewPlanningAdapterError(
                "mention wake count and window must be supplied together"
            )
        if count is not None:
            object.__setattr__(
                self,
                "mention_wake_count",
                _positive_int(count, field_name="mention_wake_count"),
            )
            object.__setattr__(
                self,
                "mention_wake_window_seconds",
                _positive_finite(
                    window,
                    field_name="mention_wake_window_seconds",
                ),
            )
        for field_name in ("model_execution_id", "prompt_signature"):
            object.__setattr__(
                self,
                field_name,
                _optional_diagnostic_text(getattr(self, field_name), field_name=field_name),
            )
        failure_code = _optional_diagnostic_text(
            self.failure_code,
            field_name="failure_code",
        )
        failure_message = _optional_diagnostic_text(
            self.failure_message,
            field_name="failure_message",
        )
        if failure_message and not failure_code:
            raise IdleReviewPlanningAdapterError(
                "failure_message requires a failure_code"
            )
        if failure_code and (
            self.next_review_after_seconds is not None
            or self.mention_sensitivity is not None
            or self.mention_wake_count is not None
        ):
            raise IdleReviewPlanningAdapterError(
                "failed planner output cannot include scheduling controls"
            )
        object.__setattr__(self, "failure_code", failure_code)
        object.__setattr__(self, "failure_message", failure_message)

    @classmethod
    def from_stage_output(
        cls,
        output: object,
    ) -> IdleReviewPlanningWorkflowOutput:
        """Convert the existing runner-local output into an actor result."""

        try:
            next_review_after_seconds = output.next_review_after_seconds  # type: ignore[attr-defined]
            reason = output.reason  # type: ignore[attr-defined]
            mention_sensitivity = output.mention_sensitivity  # type: ignore[attr-defined]
            mention_wake_count = output.mention_wake_count  # type: ignore[attr-defined]
            mention_wake_window_seconds = output.mention_wake_window_seconds  # type: ignore[attr-defined]
        except AttributeError as exc:
            raise TypeError("planner runner returned an invalid output type") from exc
        count = mention_wake_count
        window = mention_wake_window_seconds
        if (count is None) != (window is None):
            count = None
            window = None
        return cls(
            next_review_after_seconds=next_review_after_seconds,
            reason=reason or "idle_review_planning_defaulted",
            mention_sensitivity=mention_sensitivity,
            mention_wake_count=count,
            mention_wake_window_seconds=window,
            model_execution_id=getattr(output, "model_execution_id", ""),
            prompt_signature=getattr(output, "prompt_signature", ""),
            failure_code=getattr(output, "failure_code", ""),
            failure_message=getattr(output, "failure_message", ""),
        )

    def to_completion_payload(self) -> dict[str, object]:
        """Return the only model-controlled data accepted by the reducer."""

        outcome: dict[str, object] = {
            "kind": (
                "failed"
                if self.failure_code
                else (
                    "planned"
                    if self.next_review_after_seconds is not None
                    else "defaulted"
                )
            ),
            "requested_delay_seconds": self.next_review_after_seconds,
            "reason": self.reason,
            "mention_sensitivity": (
                self.mention_sensitivity.value
                if self.mention_sensitivity is not None
                else MentionSensitivity.NORMAL.value
            ),
            "active_reply_threshold": (
                {
                    "at_count": self.mention_wake_count,
                    "window_seconds": self.mention_wake_window_seconds,
                }
                if self.mention_wake_count is not None
                else {}
            ),
        }
        payload: dict[str, object] = {"outcome": outcome}
        if self.model_execution_id:
            payload["model_execution_id"] = self.model_execution_id
        if self.prompt_signature:
            payload["prompt_signature"] = self.prompt_signature
        if self.failure_code:
            payload["failure_code"] = self.failure_code
            payload["failure_message"] = self.failure_message
        return payload


class IdleReviewPlanningWorkflowPort(Protocol):
    """Run one pure model decision without mutating runtime state."""

    async def run_idle_review_planning(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> IdleReviewPlanningWorkflowOutput:
        """Return a bounded relative review proposal for one actor effect."""


class IdleReviewPlanningStageRunner(Protocol):
    """Structural runner boundary that avoids importing the runner package here."""

    async def run(self, stage_input: ReviewStageInput) -> object:
        """Return one runner-local planning output."""


class IdleReviewPlanningStageInputProjector(Protocol):
    """Build actor-owned prompt input from a durable planner descriptor."""

    async def build_idle_review_planning_stage_input(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> ReviewStageInput:
        """Return an immutable-boundary review-stage input."""


class RunnerIdleReviewPlanningWorkflow:
    """Bridge an existing stage runner through a durable actor projector."""

    def __init__(
        self,
        *,
        projector: IdleReviewPlanningStageInputProjector,
        runner: IdleReviewPlanningStageRunner,
    ) -> None:
        self._projector = projector
        self._runner = runner

    async def run_idle_review_planning(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> IdleReviewPlanningWorkflowOutput:
        """Project durable context, invoke the runner, and normalize its output."""

        stage_input = await self._projector.build_idle_review_planning_stage_input(request)
        if not isinstance(stage_input, ReviewStageInput):
            raise IdleReviewPlanningAdapterError(
                "idle review planning projector returned an invalid stage input"
            )
        if stage_input.session_id != request.effect.key.session_id:
            raise IdleReviewPlanningAdapterError(
                "idle review planning projector changed session_id"
            )
        if stage_input.purpose != "idle_review_planning":
            raise IdleReviewPlanningAdapterError(
                "idle review planning projector changed stage purpose"
            )
        output = await self._runner.run(stage_input)
        return IdleReviewPlanningWorkflowOutput.from_stage_output(output)


class IdleReviewPlanningEffectHandler:
    """Run a pure planner and return no data outside the scheduling proposal."""

    def __init__(self, *, workflow: IdleReviewPlanningWorkflowPort) -> None:
        self._workflow = workflow

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Translate one claimed effect into a fenced planner completion."""

        if context.effect.kind != _IDLE_REVIEW_PLANNING_EFFECT_KIND:
            raise IdleReviewPlanningAdapterError(
                "idle review planning adapter expected "
                f"{_IDLE_REVIEW_PLANNING_EFFECT_KIND!r}, got "
                f"{context.effect.kind!r}"
            )
        if context.effect.contract_version == _LEGACY_BYPASS_CONTRACT_VERSION:
            return EffectHandlerResult(
                payload={
                    "outcome": {
                        "kind": "bypassed",
                        "requested_delay_seconds": None,
                        "reason": "legacy_idle_review_planning_v1_bypassed",
                        "mention_sensitivity": MentionSensitivity.NORMAL.value,
                        "active_reply_threshold": {},
                    }
                }
            )
        if context.effect.contract_version not in {
            _DESCRIPTOR_CONTRACT_VERSION,
            _ACTOR_NATIVE_CANCELLABLE_CONTRACT_VERSION,
        }:
            raise IdleReviewPlanningAdapterError(
                "unsupported idle review planning contract version: "
                f"{context.effect.contract_version}"
            )
        effect_input = IdleReviewPlanningEffectInput.from_effect_context(context)
        output = await self._workflow.run_idle_review_planning(
            IdleReviewPlanningWorkflowRequest(effect=effect_input)
        )
        if not isinstance(output, IdleReviewPlanningWorkflowOutput):
            raise TypeError("idle review planning workflow returned an invalid output type")
        return EffectHandlerResult(payload=output.to_completion_payload())


def register_idle_review_planning_effect_handler(
    registry: EffectHandlerRegistry,
    *,
    workflow: IdleReviewPlanningWorkflowPort,
) -> IdleReviewPlanningEffectHandler:
    """Register one planner handler for every built-in contract version."""

    handler = IdleReviewPlanningEffectHandler(workflow=workflow)
    for contract in builtin_session_actor_effect_contracts():
        if (
            contract.effect_kind != _IDLE_REVIEW_PLANNING_EFFECT_KIND
            or contract.version not in _SUPPORTED_EFFECT_CONTRACT_VERSIONS
        ):
            continue
        registry.register(
            _IDLE_REVIEW_PLANNING_EFFECT_KIND,
            handler,
            contract=contract,
        )
    return handler


def _required_object(value: object, *, field_name: str) -> Mapping[str, Any]:
    """Return one object without retaining arbitrary mapping implementations."""

    if not isinstance(value, Mapping):
        raise IdleReviewPlanningAdapterError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _required_text(value: object, *, field_name: str) -> str:
    """Return bounded non-empty text without implicit conversion."""

    if not isinstance(value, str):
        raise IdleReviewPlanningAdapterError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise IdleReviewPlanningAdapterError(f"{field_name} must not be empty")
    if len(normalized) > _MAX_DIAGNOSTIC_TEXT_LENGTH:
        raise IdleReviewPlanningAdapterError(
            f"{field_name} exceeds {_MAX_DIAGNOSTIC_TEXT_LENGTH} chars"
        )
    return normalized


def _optional_diagnostic_text(value: object, *, field_name: str) -> str:
    """Normalize bounded optional execution diagnostics."""

    if value is None:
        return ""
    if not isinstance(value, str):
        raise IdleReviewPlanningAdapterError(f"{field_name} must be a string")
    normalized = value.strip()
    if len(normalized) > _MAX_DIAGNOSTIC_TEXT_LENGTH:
        raise IdleReviewPlanningAdapterError(
            f"{field_name} exceeds {_MAX_DIAGNOSTIC_TEXT_LENGTH} chars"
        )
    return normalized


def _nonnegative_int(value: object, *, field_name: str) -> int:
    """Return a non-negative integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise IdleReviewPlanningAdapterError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    """Return a positive integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise IdleReviewPlanningAdapterError(
            f"{field_name} must be a positive integer"
        )
    return value


def _optional_nonnegative_int(value: object, *, field_name: str) -> int | None:
    """Return an optional non-negative integer without truthy coercion."""

    if value is None:
        return None
    return _nonnegative_int(value, field_name=field_name)


def _positive_finite(value: object, *, field_name: str) -> float:
    """Return one finite positive number without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IdleReviewPlanningAdapterError(f"{field_name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise IdleReviewPlanningAdapterError(
            f"{field_name} must be a finite positive number"
        )
    return normalized


__all__ = [
    "IdleReviewPlanningAdapterError",
    "IdleReviewPlanningEffectHandler",
    "IdleReviewPlanningEffectInput",
    "IdleReviewPlanningStageInputProjector",
    "IdleReviewPlanningWorkflowOutput",
    "IdleReviewPlanningWorkflowPort",
    "IdleReviewPlanningWorkflowRequest",
    "RunnerIdleReviewPlanningWorkflow",
    "register_idle_review_planning_effect_handler",
]
