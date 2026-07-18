"""Actor-native handler for the fenced review-cancellation control effect."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from shinbot.agent.runtime.session_actor.effect_contracts import builtin_effect_contract
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.execution_control import (
    ReviewCancellationControlPort,
    ReviewCancellationGateRequest,
    ReviewCancellationGateStatus,
    ReviewCancellationQuiescencePending,
)

_REVIEW_CANCELLATION_EFFECT_KIND = "cancel_review_workflow"
_REVIEW_CANCELLATION_SUPPORTED_VERSION = 2
_REVIEW_WORKFLOW_EFFECT_KIND = "run_review_workflow"


class ReviewCancellationControlHandlerError(ValueError):
    """Raised when a review control effect cannot prove target identity."""


class ReviewCancellationControlEffectHandler:
    """Release active reply only after a pre-committed review gate is quiescent."""

    def __init__(self, *, control: ReviewCancellationControlPort) -> None:
        """Bind the executor-owned durable cancellation proof port."""

        self._control = control

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Observe the exact review cancellation gate for this control effect."""

        request = review_cancellation_request_from_context(context)
        observation = await self._control.ensure_review_cancelled(request)
        if observation.status is ReviewCancellationGateStatus.PENDING:
            raise ReviewCancellationQuiescencePending(observation)
        return EffectHandlerResult(
            payload={"review_cancellation": observation.to_payload()}
        )


def register_review_cancellation_control_effect_handler(
    registry: EffectHandlerRegistry,
    *,
    control: ReviewCancellationControlPort,
) -> ReviewCancellationControlEffectHandler:
    """Register only the current v2 review-cancellation contract.

    V1 rows lack the exact gate declaration introduced for Actor v2 review
    quiescence.  They remain known-but-unhandled until a separate historical
    maintenance path can prove their shape.
    """

    contract = builtin_effect_contract(
        _REVIEW_CANCELLATION_EFFECT_KIND,
        version=_REVIEW_CANCELLATION_SUPPORTED_VERSION,
    )
    handler = ReviewCancellationControlEffectHandler(control=control)
    registry.register(
        _REVIEW_CANCELLATION_EFFECT_KIND,
        handler,
        contract=contract,
    )
    return handler


def review_cancellation_request_from_context(
    context: EffectExecutionContext,
) -> ReviewCancellationGateRequest:
    """Decode the target review fence embedded in a v2 control effect."""

    effect = context.effect
    if (
        effect.kind != _REVIEW_CANCELLATION_EFFECT_KIND
        or effect.contract_version != _REVIEW_CANCELLATION_SUPPORTED_VERSION
    ):
        raise ReviewCancellationControlHandlerError(
            "review cancellation handler received an unsupported contract"
        )
    contract = builtin_effect_contract(
        _REVIEW_CANCELLATION_EFFECT_KIND,
        version=_REVIEW_CANCELLATION_SUPPORTED_VERSION,
    )
    if effect.contract_signature != contract.signature:
        raise ReviewCancellationControlHandlerError(
            "review cancellation contract signature changed identity"
        )
    payload = effect.payload
    operation_id = _required_text(payload, "operation_id")
    if operation_id != effect.operation_id:
        raise ReviewCancellationControlHandlerError(
            "review cancellation payload operation_id changed identity"
        )
    fence = _required_mapping(payload, "cancelled_operation_fence")
    review_operation_id = _required_text(fence, "operation_id")
    if review_operation_id != operation_id:
        raise ReviewCancellationControlHandlerError(
            "cancelled review fence operation_id changed identity"
        )
    review_effect_id = _required_text(fence, "effect_id")
    review_effect_kind = _required_text(fence, "effect_kind")
    if review_effect_kind != _REVIEW_WORKFLOW_EFFECT_KIND:
        raise ReviewCancellationControlHandlerError(
            "cancelled review fence targets a different workflow kind"
        )
    review_contract_version = _positive_int(fence, "contract_version")
    review_contract_signature = _required_text(fence, "contract_signature")
    try:
        review_contract = builtin_effect_contract(
            review_effect_kind,
            version=review_contract_version,
        )
    except KeyError as exc:
        raise ReviewCancellationControlHandlerError(
            "cancelled review fence references an unsupported workflow contract"
        ) from exc
    if review_contract.signature != review_contract_signature:
        raise ReviewCancellationControlHandlerError(
            "cancelled review fence contract signature changed identity"
        )
    if _positive_int(fence, "ownership_generation") != effect.ownership_generation:
        raise ReviewCancellationControlHandlerError(
            "cancelled review fence ownership generation changed identity"
        )
    return ReviewCancellationGateRequest(
        key=effect.key,
        ownership_generation=effect.ownership_generation,
        cancellation_effect_id=effect.effect_id,
        request_event_id=effect.source_event_id,
        review_operation_id=review_operation_id,
        review_effect_id=review_effect_id,
        review_effect_kind=review_effect_kind,
        review_contract_version=review_contract_version,
        review_contract_signature=review_contract_signature,
    )


def _required_mapping(payload: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise ReviewCancellationControlHandlerError(f"{field_name} must be an object")
    return value


def _required_text(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ReviewCancellationControlHandlerError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ReviewCancellationControlHandlerError(f"{field_name} must not be empty")
    return normalized


def _positive_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ReviewCancellationControlHandlerError(
            f"{field_name} must be a positive integer"
        )
    return value


__all__ = [
    "ReviewCancellationControlEffectHandler",
    "ReviewCancellationControlHandlerError",
    "register_review_cancellation_control_effect_handler",
    "review_cancellation_request_from_context",
]
