"""Tests for the Actor v2 idle-review planning effect boundary."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from shinbot.agent.runners.review_models import IdleReviewPlanningStageOutput
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    EffectExecutionContext,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.idle_review_planning import (
    IdleReviewPlanningInput,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    IdleReviewPlanningAdapterError,
    IdleReviewPlanningEffectHandler,
    IdleReviewPlanningEffectInput,
    IdleReviewPlanningWorkflowOutput,
    IdleReviewPlanningWorkflowRequest,
    RunnerIdleReviewPlanningWorkflow,
    register_idle_review_planning_effect_handler,
)
from shinbot.agent.scheduler.models import MentionSensitivity
from shinbot.agent.services.context.review_context_builder import ReviewStageInput

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


class _UnusedEffectStore:
    """Minimal store because adapter tests do not renew an effect lease."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Return the existing claim for an unexpected renewal call."""

        return claim


@dataclass(slots=True)
class _Workflow:
    """Pure planner workflow fake that records the actor request."""

    output: IdleReviewPlanningWorkflowOutput
    requests: list[IdleReviewPlanningWorkflowRequest] = field(default_factory=list)

    async def run_idle_review_planning(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> IdleReviewPlanningWorkflowOutput:
        """Return the configured planner output."""

        self.requests.append(request)
        return self.output


@dataclass(slots=True)
class _Projector:
    """Read-only durable-context projector fake for the runner bridge."""

    stage_input: ReviewStageInput
    requests: list[IdleReviewPlanningWorkflowRequest] = field(default_factory=list)

    async def build_idle_review_planning_stage_input(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> ReviewStageInput:
        """Record the request and return its configured stage input."""

        self.requests.append(request)
        return self.stage_input


@dataclass(slots=True)
class _Runner:
    """Existing-stage-runner fake used by the actor bridge test."""

    output: IdleReviewPlanningStageOutput
    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> IdleReviewPlanningStageOutput:
        """Record one prompt input and return the configured stage output."""

        self.inputs.append(stage_input)
        return self.output


def _planning_input(*, input_watermark: int = 25) -> IdleReviewPlanningInput:
    """Build the canonical actor-derived descriptor used by one effect."""

    return IdleReviewPlanningInput(
        input_watermark=input_watermark,
        active_epoch=3,
        activity_generation=6,
        trigger="active_chat_decay",
        active_chat_interest=4.0,
        active_chat_entered_at=100.0,
        active_chat_last_message_at=110.0,
        active_chat_tick_count=2,
        active_chat_bootstrap_disposition="continue",
    )


def _effect(
    *,
    planning_input: IdleReviewPlanningInput | None = None,
    payload_updates: dict[str, object] | None = None,
    contract_version: int | None = None,
) -> DurableEffectEnvelope:
    """Build a claimed planner effect with actor-owned fence fields."""

    descriptor = planning_input or _planning_input()
    contract = builtin_effect_contract(
        "run_idle_review_planning",
        version=contract_version,
    )
    payload: dict[str, object] = {
        "plan_id": "plan-a",
        "active_epoch": 3,
        "activity_generation": 6,
        "input_watermark": 25,
        "input_ledger_sequence": None,
        "completion_event_id": "completion-a",
        "failure_event_id": "failure-a",
        "source": "session_actor",
        "trigger": "active_chat_decay",
        "planning_input": descriptor.to_payload(),
    }
    payload.update(payload_updates or {})
    return DurableEffectEnvelope(
        effect_id="effect-a",
        key=_KEY,
        kind="run_idle_review_planning",
        idempotency_key="effect-a",
        ownership_generation=1,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload=payload,
        source_event_id="exit-a",
        operation_id="operation-a",
        trace_id="trace-a",
    )


def _context(effect: DurableEffectEnvelope) -> EffectExecutionContext:
    """Create a claim context without involving the durable effect executor."""

    return EffectExecutionContext(
        _UnusedEffectStore(),  # type: ignore[arg-type]
        ClaimedEffect(
            claim_id="claim-a",
            effect=effect,
            worker_id="worker-a",
            attempt_count=1,
        ),
    )


@pytest.mark.asyncio
async def test_handler_returns_only_model_controlled_schedule_outcome() -> None:
    """Planner output cannot replace actor effect identity or provenance."""

    workflow = _Workflow(
        IdleReviewPlanningWorkflowOutput(
            next_review_after_seconds=45.0,
            reason="conversation_settled",
            mention_sensitivity=MentionSensitivity.HIGH,
            mention_wake_count=2,
            mention_wake_window_seconds=90.0,
            model_execution_id="execution-a",
            prompt_signature="prompt-a",
        )
    )

    result = await IdleReviewPlanningEffectHandler(workflow=workflow)(
        _context(_effect())
    )

    assert len(workflow.requests) == 1
    request = workflow.requests[0]
    assert request.effect.key == _KEY
    assert request.effect.operation_id == "operation-a"
    assert request.effect.plan_id == "plan-a"
    assert request.effect.planning_input.input_watermark == 25
    assert set(result.payload) == {
        "outcome",
        "model_execution_id",
        "prompt_signature",
    }
    assert result.payload["model_execution_id"] == "execution-a"
    assert result.payload["prompt_signature"] == "prompt-a"
    assert result.payload["outcome"] == {
        "kind": "planned",
        "requested_delay_seconds": 45.0,
        "reason": "conversation_settled",
        "mention_sensitivity": "high",
        "active_reply_threshold": {"at_count": 2, "window_seconds": 90.0},
    }


@pytest.mark.asyncio
async def test_handler_rejects_descriptor_that_does_not_match_effect_fence() -> None:
    """A persisted descriptor cannot widen or change the captured snapshot."""

    workflow = _Workflow(IdleReviewPlanningWorkflowOutput())
    effect = _effect(planning_input=_planning_input(input_watermark=24))

    with pytest.raises(
        IdleReviewPlanningAdapterError,
        match="planning_input changed input_watermark",
    ):
        await IdleReviewPlanningEffectHandler(workflow=workflow)(_context(effect))

    assert workflow.requests == []


@pytest.mark.asyncio
async def test_v1_effect_bypasses_untrusted_legacy_planning_input() -> None:
    """Historical v1 effects settle safely without reusing arbitrary prompt data."""

    workflow = _Workflow(IdleReviewPlanningWorkflowOutput())
    result = await IdleReviewPlanningEffectHandler(workflow=workflow)(
        _context(
            _effect(
                contract_version=1,
                payload_updates={"planning_input": {"legacy": "untrusted"}},
            )
        )
    )

    assert workflow.requests == []
    assert result.payload == {
        "outcome": {
            "kind": "bypassed",
            "requested_delay_seconds": None,
            "reason": "legacy_idle_review_planning_v1_bypassed",
            "mention_sensitivity": "normal",
            "active_reply_threshold": {},
        }
    }


@pytest.mark.asyncio
async def test_runner_bridge_projects_durable_context_before_model_call() -> None:
    """The existing runner receives only a projector-built Actor v2 stage input."""

    effect = _effect()
    request = IdleReviewPlanningWorkflowRequest(
        effect=IdleReviewPlanningEffectInput.from_effect_context(_context(effect))
    )
    stage_input = ReviewStageInput(
        session_id=_KEY.session_id,
        purpose="idle_review_planning",
        source_messages=[],
        context_messages=[{"role": "user", "content": "durable projection"}],
        metadata={"input_watermark": 25},
    )
    projector = _Projector(stage_input)
    runner = _Runner(
        IdleReviewPlanningStageOutput(
            next_review_after_seconds=30.0,
            reason="quiet_after_active_chat",
            mention_sensitivity=MentionSensitivity.LOW,
            mention_wake_count=1,
            mention_wake_window_seconds=60.0,
            model_execution_id="execution-a",
            prompt_signature="prompt-a",
        )
    )

    output = await RunnerIdleReviewPlanningWorkflow(
        projector=projector,
        runner=runner,
    ).run_idle_review_planning(request)

    assert projector.requests == [request]
    assert runner.inputs == [stage_input]
    assert output.next_review_after_seconds == 30.0
    assert output.reason == "quiet_after_active_chat"
    assert output.mention_sensitivity is MentionSensitivity.LOW
    assert output.model_execution_id == "execution-a"
    assert output.prompt_signature == "prompt-a"


@pytest.mark.asyncio
async def test_runner_bridge_preserves_an_explicit_model_failure() -> None:
    """A runner failure becomes a failed actor outcome, never a default decision."""

    effect = _effect()
    request = IdleReviewPlanningWorkflowRequest(
        effect=IdleReviewPlanningEffectInput.from_effect_context(_context(effect))
    )
    stage_input = ReviewStageInput(
        session_id=_KEY.session_id,
        purpose="idle_review_planning",
        source_messages=[],
    )
    output = await RunnerIdleReviewPlanningWorkflow(
        projector=_Projector(stage_input),
        runner=_Runner(
            IdleReviewPlanningStageOutput(
                reason="llm_idle_review_planning_failed",
                failure_code="model_output_unavailable",
                failure_message="no structured output",
            )
        ),
    ).run_idle_review_planning(request)

    assert output.to_completion_payload() == {
        "outcome": {
            "kind": "failed",
            "requested_delay_seconds": None,
            "reason": "llm_idle_review_planning_failed",
            "mention_sensitivity": "normal",
            "active_reply_threshold": {},
        },
        "failure_code": "model_output_unavailable",
        "failure_message": "no structured output",
    }


def test_registration_covers_each_builtin_planner_contract_version() -> None:
    """Future activation receives the same handler for every planner contract."""

    workflow = _Workflow(IdleReviewPlanningWorkflowOutput())
    registry = EffectHandlerRegistry()

    handler = register_idle_review_planning_effect_handler(registry, workflow=workflow)

    contracts = tuple(
        contract
        for contract in builtin_session_actor_effect_contracts()
        if contract.effect_kind == "run_idle_review_planning"
    )
    assert contracts
    for contract in contracts:
        registered, registered_handler = registry.resolve(
            "run_idle_review_planning",
            contract.version,
        )
        assert registered == contract
        assert registered_handler is handler
