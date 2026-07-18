"""Tests for the strict Actor v2 review-cancellation control handler."""

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
from shinbot.agent.runtime.session_actor.execution_control import (
    ReviewCancellationGateObservation,
    ReviewCancellationGateRequest,
    ReviewCancellationGateStatus,
)
from shinbot.agent.runtime.session_actor.execution_control_handler import (
    ReviewCancellationControlHandlerError,
    register_review_cancellation_control_effect_handler,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


@dataclass(slots=True)
class _ControlPort:
    """Capture the exact request selected by the handler."""

    observation: ReviewCancellationGateObservation
    requests: list[ReviewCancellationGateRequest]

    async def ensure_review_cancelled(
        self,
        request: ReviewCancellationGateRequest,
    ) -> ReviewCancellationGateObservation:
        self.requests.append(request)
        return self.observation


@dataclass(slots=True)
class _Context:
    """Minimal immutable effect context for direct handler testing."""

    effect: DurableEffectEnvelope


def _context(
    *,
    payload: dict[str, object] | None = None,
) -> EffectExecutionContext:
    cancellation_contract = builtin_effect_contract("cancel_review_workflow", version=2)
    review_contract = builtin_effect_contract("run_review_workflow", version=2)
    return cast(
        EffectExecutionContext,
        _Context(
            effect=DurableEffectEnvelope(
                effect_id="effect:cancel-review",
                key=_KEY,
                kind="cancel_review_workflow",
                idempotency_key="effect:cancel-review",
                ownership_generation=1,
                contract_version=2,
                contract_signature=cancellation_contract.signature,
                operation_id="review-operation-a",
                source_event_id="message:priority",
                payload={
                    "operation_id": "review-operation-a",
                    "plan_id": "plan-a",
                    "active_epoch": 4,
                    "activity_generation": 7,
                    "input_watermark": 12,
                    "input_ledger_sequence": 12,
                    "completion_event_id": "effect:cancel-review:completed",
                    "failure_event_id": "effect:cancel-review:failed",
                    "superseded_by_event_id": "message:priority",
                    "cancelled_operation_fence": {
                        "operation_id": "review-operation-a",
                        "effect_id": "effect:review",
                        "effect_kind": "run_review_workflow",
                        "contract_version": 2,
                        "contract_signature": review_contract.signature,
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
    register_review_cancellation_control_effect_handler(registry, control=port)
    return registry


@pytest.mark.asyncio
async def test_review_control_waits_for_a_confirmed_fenced_gate() -> None:
    port = _ControlPort(
        observation=ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.CONFIRMED,
            cancellation_effect_id="effect:cancel-review",
            review_effect_id="effect:review",
        ),
        requests=[],
    )
    registry = _registry(port)
    _, handler = registry.resolve("cancel_review_workflow", 2)

    result = await handler(_context())

    assert len(port.requests) == 1
    request = port.requests[0]
    assert request.cancellation_effect_id == "effect:cancel-review"
    assert request.request_event_id == "message:priority"
    assert request.review_operation_id == "review-operation-a"
    assert request.review_effect_id == "effect:review"
    assert request.review_contract_version == 2
    assert result.payload["review_cancellation"]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_pending_gate_retries_without_a_completion() -> None:
    port = _ControlPort(
        observation=ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.PENDING,
            cancellation_effect_id="effect:cancel-review",
            review_effect_id="effect:review",
            durable_running_count=1,
            blocker_code="review_execution_running",
        ),
        requests=[],
    )
    registry = _registry(port)
    _, handler = registry.resolve("cancel_review_workflow", 2)

    with pytest.raises(RuntimeError, match="quiescence remains pending"):
        await handler(_context())

    assert len(port.requests) == 1


@pytest.mark.asyncio
async def test_unknown_gate_completes_as_a_durable_blocker() -> None:
    port = _ControlPort(
        observation=ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.BLOCKED,
            cancellation_effect_id="effect:cancel-review",
            review_effect_id="effect:review",
            durable_unknown_count=1,
            blocker_code="review_execution_witness_unknown",
        ),
        requests=[],
    )
    registry = _registry(port)
    _, handler = registry.resolve("cancel_review_workflow", 2)

    result = await handler(_context())

    assert len(port.requests) == 1
    assert result.payload["review_cancellation"] == {
        "status": "blocked",
        "cancellation_effect_id": "effect:cancel-review",
        "review_effect_id": "effect:review",
        "local_task_count": 0,
        "durable_running_count": 0,
        "durable_unknown_count": 1,
        "blocker_code": "review_execution_witness_unknown",
    }


@pytest.mark.asyncio
async def test_control_rejects_a_mutated_review_fence_before_port_call() -> None:
    port = _ControlPort(
        observation=ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.CONFIRMED,
            cancellation_effect_id="effect:cancel-review",
            review_effect_id="effect:review",
        ),
        requests=[],
    )
    registry = _registry(port)
    _, handler = registry.resolve("cancel_review_workflow", 2)

    with pytest.raises(
        ReviewCancellationControlHandlerError,
        match="different workflow kind",
    ):
        await handler(
            _context(
                payload={
                    "cancelled_operation_fence": {
                        **_context().effect.payload["cancelled_operation_fence"],
                        "effect_kind": "run_active_reply_workflow",
                    }
                }
            )
        )

    assert port.requests == []


def test_registration_covers_only_current_fenced_review_control() -> None:
    port = _ControlPort(
        observation=ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.CONFIRMED,
            cancellation_effect_id="effect:cancel-review",
            review_effect_id="effect:review",
        ),
        requests=[],
    )
    registry = _registry(port)

    assert registry.handled_contracts() == (
        builtin_effect_contract("cancel_review_workflow", version=2),
    )
