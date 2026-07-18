"""Actor-native handler for generic model-execution cancellation gates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from shinbot.agent.runtime.session_actor.effect_contracts import builtin_effect_contract
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION,
    MODEL_EXECUTION_CANCELLATION_EFFECT_KIND,
    ModelExecutionCancellationControlPort,
    ModelExecutionCancellationGateRequest,
    ModelExecutionCancellationGateStatus,
    ModelExecutionCancellationQuiescencePending,
    is_model_execution_cancellation_target,
)


class ModelExecutionCancellationControlHandlerError(ValueError):
    """Raised when a v3 control effect widens or changes its target fence."""


class ModelExecutionCancellationControlEffectHandler:
    """Complete only once a generic model cancellation is proven or blocked."""

    def __init__(self, *, control: ModelExecutionCancellationControlPort) -> None:
        """Bind the executor-owned durable cancellation proof port."""

        self._control = control

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Observe the exact gate declared by this durable control effect."""

        request = model_execution_cancellation_request_from_context(context)
        observation = await self._control.ensure_model_execution_cancelled(request)
        if observation.status is ModelExecutionCancellationGateStatus.PENDING:
            raise ModelExecutionCancellationQuiescencePending(observation)
        return EffectHandlerResult(
            payload={"model_execution_cancellation": observation.to_payload()}
        )


def register_model_execution_cancellation_control_effect_handler(
    registry: EffectHandlerRegistry,
    *,
    control: ModelExecutionCancellationControlPort,
) -> ModelExecutionCancellationControlEffectHandler:
    """Register only the new v3 generic cancellation control contract."""

    contract = builtin_effect_contract(
        MODEL_EXECUTION_CANCELLATION_EFFECT_KIND,
        version=MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION,
    )
    handler = ModelExecutionCancellationControlEffectHandler(control=control)
    registry.register(
        MODEL_EXECUTION_CANCELLATION_EFFECT_KIND,
        handler,
        contract=contract,
    )
    return handler


def model_execution_cancellation_request_from_context(
    context: EffectExecutionContext,
) -> ModelExecutionCancellationGateRequest:
    """Decode the sealed v3 target fence carried by a control effect."""

    effect = context.effect
    if (
        effect.kind != MODEL_EXECUTION_CANCELLATION_EFFECT_KIND
        or effect.contract_version != MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION
    ):
        raise ModelExecutionCancellationControlHandlerError(
            "model execution cancellation handler received an unsupported contract"
        )
    control_contract = builtin_effect_contract(
        MODEL_EXECUTION_CANCELLATION_EFFECT_KIND,
        version=MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION,
    )
    if effect.contract_signature != control_contract.signature:
        raise ModelExecutionCancellationControlHandlerError(
            "model execution cancellation control signature changed identity"
        )
    payload = effect.payload
    operation_id = _required_text(payload, "operation_id")
    if operation_id != effect.operation_id:
        raise ModelExecutionCancellationControlHandlerError(
            "model execution cancellation payload operation_id changed identity"
        )
    fence = _required_mapping(payload, "cancelled_model_effect_fence")
    target_operation_id = _required_text(fence, "operation_id")
    if target_operation_id != operation_id:
        raise ModelExecutionCancellationControlHandlerError(
            "cancelled model target operation_id changed identity"
        )
    target_effect_id = _required_text(fence, "effect_id")
    target_effect_kind = _required_text(fence, "effect_kind")
    target_contract_version = _positive_int(fence, "contract_version")
    target_contract_signature = _required_text(fence, "contract_signature")
    if not is_model_execution_cancellation_target(
        effect_kind=target_effect_kind,
        contract_version=target_contract_version,
    ):
        raise ModelExecutionCancellationControlHandlerError(
            "cancelled model target has not opted into v3 cancellation"
        )
    try:
        target_contract = builtin_effect_contract(
            target_effect_kind,
            version=target_contract_version,
        )
    except KeyError as exc:
        raise ModelExecutionCancellationControlHandlerError(
            "cancelled model target contract is unsupported"
        ) from exc
    if target_contract.signature != target_contract_signature:
        raise ModelExecutionCancellationControlHandlerError(
            "cancelled model target contract signature changed identity"
        )
    if _positive_int(fence, "ownership_generation") != effect.ownership_generation:
        raise ModelExecutionCancellationControlHandlerError(
            "cancelled model target ownership generation changed identity"
        )
    return ModelExecutionCancellationGateRequest(
        key=effect.key,
        ownership_generation=effect.ownership_generation,
        cancellation_effect_id=effect.effect_id,
        request_event_id=effect.source_event_id,
        target_operation_id=target_operation_id,
        target_effect_id=target_effect_id,
        target_effect_kind=target_effect_kind,
        target_contract_version=target_contract_version,
        target_contract_signature=target_contract_signature,
    )


def _required_mapping(payload: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise ModelExecutionCancellationControlHandlerError(
            f"{field_name} must be an object"
        )
    return value


def _required_text(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ModelExecutionCancellationControlHandlerError(
            f"{field_name} must be a string"
        )
    normalized = value.strip()
    if not normalized:
        raise ModelExecutionCancellationControlHandlerError(
            f"{field_name} must not be empty"
        )
    return normalized


def _positive_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ModelExecutionCancellationControlHandlerError(
            f"{field_name} must be a positive integer"
        )
    return value


__all__ = [
    "ModelExecutionCancellationControlEffectHandler",
    "ModelExecutionCancellationControlHandlerError",
    "model_execution_cancellation_request_from_context",
    "register_model_execution_cancellation_control_effect_handler",
]
