"""Unit coverage for the v3 generic model cancellation control handler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectEnvelope,
    EffectExecutionContext,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    ModelExecutionCancellationGateObservation,
    ModelExecutionCancellationGateRequest,
    ModelExecutionCancellationGateStatus,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_handler import (
    ModelExecutionCancellationControlHandlerError,
    register_model_execution_cancellation_control_effect_handler,
)

_KEY = SessionKey("profile-model-gate", "bot:group:model-gate")


@dataclass(slots=True)
class _ControlPort:
    """Capture the exact immutable request emitted by the control handler."""

    observation: ModelExecutionCancellationGateObservation
    requests: list[ModelExecutionCancellationGateRequest]

    async def ensure_model_execution_cancelled(
        self,
        request: ModelExecutionCancellationGateRequest,
    ) -> ModelExecutionCancellationGateObservation:
        self.requests.append(request)
        return self.observation


@dataclass(slots=True)
class _Context:
    """Minimal immutable effect context used for direct handler invocation."""

    effect: DurableEffectEnvelope


def _context(
    *,
    payload: dict[str, object] | None = None,
) -> EffectExecutionContext:
    control_contract = builtin_effect_contract("cancel_model_execution", version=3)
    target_contract = builtin_effect_contract("run_idle_review_planning", version=3)
    return cast(
        EffectExecutionContext,
        _Context(
            effect=DurableEffectEnvelope(
                effect_id="effect:cancel-model",
                key=_KEY,
                kind="cancel_model_execution",
                idempotency_key="effect:cancel-model",
                ownership_generation=1,
                contract_version=3,
                contract_signature=control_contract.signature,
                operation_id="idle-planning-operation",
                source_event_id="message:later",
                payload={
                    "operation_id": "idle-planning-operation",
                    "plan_id": "plan-a",
                    "active_epoch": 4,
                    "activity_generation": 7,
                    "input_watermark": 12,
                    "input_ledger_sequence": 12,
                    "completion_event_id": "effect:cancel-model:completed",
                    "failure_event_id": "effect:cancel-model:failed",
                    "superseded_by_event_id": "message:later",
                    "cancelled_model_effect_fence": {
                        "operation_id": "idle-planning-operation",
                        "effect_id": "effect:idle-planning",
                        "effect_kind": "run_idle_review_planning",
                        "contract_version": 3,
                        "contract_signature": target_contract.signature,
                        "ownership_generation": 1,
                    },
                    **(payload or {}),
                },
            )
        ),
    )


def _registry(port: _ControlPort) -> EffectHandlerRegistry:
    registry = EffectHandlerRegistry(
        contract_authority=builtin_effect_contract_authority()
    )
    register_model_execution_cancellation_control_effect_handler(registry, control=port)
    return registry


@pytest.mark.asyncio
async def test_handler_returns_confirmed_generic_cancellation_evidence() -> None:
    """A confirmed gate emits only its bounded durable proof."""

    port = _ControlPort(
        observation=ModelExecutionCancellationGateObservation(
            status=ModelExecutionCancellationGateStatus.CONFIRMED,
            cancellation_effect_id="effect:cancel-model",
            target_effect_id="effect:idle-planning",
            target_effect_kind="run_idle_review_planning",
            target_operation_id="idle-planning-operation",
        ),
        requests=[],
    )
    _, handler = _registry(port).resolve("cancel_model_execution", 3)

    result = await handler(_context())

    assert len(port.requests) == 1
    request = port.requests[0]
    assert request.request_event_id == "message:later"
    assert request.target_effect_id == "effect:idle-planning"
    assert request.target_contract_version == 3
    assert result.payload["model_execution_cancellation"]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_handler_defers_while_a_witnessed_target_is_still_running() -> None:
    """A running durable witness cannot be collapsed into a completion."""

    port = _ControlPort(
        observation=ModelExecutionCancellationGateObservation(
            status=ModelExecutionCancellationGateStatus.PENDING,
            cancellation_effect_id="effect:cancel-model",
            target_effect_id="effect:idle-planning",
            target_effect_kind="run_idle_review_planning",
            target_operation_id="idle-planning-operation",
            target_claim_id="target-claim",
            target_worker_id="target-worker",
            durable_running_count=1,
            blocker_code="model_execution_running",
        ),
        requests=[],
    )
    _, handler = _registry(port).resolve("cancel_model_execution", 3)

    with pytest.raises(RuntimeError, match="quiescence remains pending"):
        await handler(_context())


@pytest.mark.asyncio
async def test_handler_preserves_unknown_evidence_as_blocked_completion() -> None:
    """Unknown execution is delivered for actor-side blocking, not cancellation."""

    port = _ControlPort(
        observation=ModelExecutionCancellationGateObservation(
            status=ModelExecutionCancellationGateStatus.BLOCKED,
            cancellation_effect_id="effect:cancel-model",
            target_effect_id="effect:idle-planning",
            target_effect_kind="run_idle_review_planning",
            target_operation_id="idle-planning-operation",
            target_claim_id="target-claim",
            target_worker_id="target-worker",
            durable_unknown_count=1,
            blocker_code="model_execution_witness_unknown",
        ),
        requests=[],
    )
    _, handler = _registry(port).resolve("cancel_model_execution", 3)

    result = await handler(_context())

    assert result.payload["model_execution_cancellation"] == {
        "status": "blocked",
        "cancellation_effect_id": "effect:cancel-model",
        "target_effect_id": "effect:idle-planning",
        "target_effect_kind": "run_idle_review_planning",
        "target_operation_id": "idle-planning-operation",
        "target_claim_id": "target-claim",
        "target_worker_id": "target-worker",
        "durable_running_count": 0,
        "durable_unknown_count": 1,
        "blocker_code": "model_execution_witness_unknown",
    }


@pytest.mark.asyncio
async def test_handler_rejects_a_non_opted_in_target_before_port_call() -> None:
    """Historic v1/v2 targets cannot accidentally inherit v3 semantics."""

    port = _ControlPort(
        observation=ModelExecutionCancellationGateObservation(
            status=ModelExecutionCancellationGateStatus.CONFIRMED,
            cancellation_effect_id="effect:cancel-model",
            target_effect_id="effect:idle-planning",
            target_effect_kind="run_idle_review_planning",
            target_operation_id="idle-planning-operation",
        ),
        requests=[],
    )
    _, handler = _registry(port).resolve("cancel_model_execution", 3)
    base = _context().effect.payload["cancelled_model_effect_fence"]

    with pytest.raises(
        ModelExecutionCancellationControlHandlerError,
        match="has not opted into",
    ):
        await handler(
            _context(
                payload={
                    "cancelled_model_effect_fence": {
                        **base,
                        "contract_version": 2,
                    }
                }
            )
        )

    assert port.requests == []
