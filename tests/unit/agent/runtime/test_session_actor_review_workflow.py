"""Tests for the first Actor v2 review workflow vertical slice."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    EffectExecutionContext,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageLedgerEntry,
    MessagePriorityFlags,
)
from shinbot.agent.runtime.session_actor.review_workflow import (
    ActorReviewWorkflowError,
    RunnerReviewWorkflow,
)
from shinbot.agent.runtime.session_actor.review_workflow_context import (
    ActorReviewWorkflowContextError,
    ActorReviewWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActorWorkflowEffectInput,
    ReviewWorkflowEffectHandler,
    ReviewWorkflowRequest,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ReviewCompletionResult,
    ReviewNextReviewOutcomeKind,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput

_KEY = SessionKey("profile-a", "bot-a:instance-a:group:room-a")
_BASE_SESSION_ID = "instance-a:group:room-a"


class _UnusedEffectStore:
    """Minimal effect store because these tests do not renew a claim lease."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Return the supplied claim if a test accidentally renews it."""

        return claim


@dataclass(slots=True)
class _Ledger:
    """Captured unread snapshot fake used by the workflow effect adapter."""

    entries: tuple[MessageLedgerEntry, ...]
    calls: list[tuple[SessionKey, int, int, int]] = field(default_factory=list)

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        ownership_generation: int,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Record the operation fence and return its configured snapshot."""

        self.calls.append(
            (
                key,
                ownership_generation,
                input_watermark,
                input_ledger_sequence,
            )
        )
        return self.entries


@dataclass(slots=True)
class _MessageStore:
    """Message-log fake that deliberately returns rows out of ledger order."""

    payloads: dict[int, dict[str, object]]
    requested_ids: list[tuple[int, ...]] = field(default_factory=list)

    def list_by_ids(self, message_log_ids: tuple[int, ...]) -> list[dict[str, object]]:
        """Return authorized records in reverse order for restoration coverage."""

        self.requested_ids.append(message_log_ids)
        return [
            self.payloads[message_log_id]
            for message_log_id in reversed(message_log_ids)
            if message_log_id in self.payloads
        ]


@dataclass(slots=True)
class _ReplyRunner:
    """One reply-stage fake used to count Actor workflow model invocations."""

    output: ReplyDecisionStageOutput
    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Record one stage input and return the configured decision."""

        self.inputs.append(stage_input)
        return self.output


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    base_session_id: str = _BASE_SESSION_ID,
    ownership_generation: int = 1,
) -> MessageLedgerEntry:
    """Build one eligible unread actor-ledger entry."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=ownership_generation,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id=f"user:{message_log_id}",
        instance_id="instance-a",
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=base_session_id,
        platform="test",
        self_id="bot-a",
        priority=MessagePriorityFlags(),
        observed_at=float(ledger_sequence),
        occurred_at=float(ledger_sequence),
        event_created_at=float(ledger_sequence),
    )
    return MessageLedgerEntry(
        message=message,
        ledger_sequence=ledger_sequence,
        recorded_at=float(ledger_sequence),
        updated_at=float(ledger_sequence),
    )


def _request(
    entries: tuple[MessageLedgerEntry, ...],
) -> ReviewWorkflowRequest:
    """Build one trusted ReviewWorkflowRequest with an exact ledger snapshot."""

    return ReviewWorkflowRequest(
        effect=ActorWorkflowEffectInput(
            key=_KEY,
            operation_id="review-operation-a",
            effect_id="review-effect-a",
            idempotency_key="review-effect-a",
            source_event_id="review-due-a",
            ownership_generation=1,
            instance_id="instance-a",
            target_session_id=_BASE_SESSION_ID,
            input_watermark=20,
            input_ledger_sequence=2,
            ledger_entries=entries,
        ),
        review_plan={"plan_id": "plan-a", "plan_revision": 1},
        plan_id="plan-a",
        plan_revision=1,
    )


def _effect() -> DurableEffectEnvelope:
    """Build a claimed review effect matching the actor request helper."""

    contract = builtin_effect_contract("run_review_workflow")
    return DurableEffectEnvelope(
        effect_id="review-effect-a",
        key=_KEY,
        kind="run_review_workflow",
        idempotency_key="review-effect-a",
        ownership_generation=1,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload={
            "operation_id": "review-operation-a",
            "effect_id": "review-effect-a",
            "effect_kind": "run_review_workflow",
            "idempotency_key": "review-effect-a",
            "source_event_id": "review-due-a",
            "ownership_generation": 1,
            "input_watermark": 20,
            "input_ledger_sequence": 2,
            "instance_id": "instance-a",
            "target_session_id": _BASE_SESSION_ID,
            "plan_id": "plan-a",
            "plan_revision": 1,
            "review_plan": {"plan_id": "plan-a", "plan_revision": 1},
        },
        source_event_id="review-due-a",
        operation_id="review-operation-a",
        trace_id="trace-review-a",
    )


def _context(effect: DurableEffectEnvelope) -> EffectExecutionContext:
    """Build an effect context without starting an executor worker."""

    return EffectExecutionContext(
        _UnusedEffectStore(),  # type: ignore[arg-type]
        ClaimedEffect(
            claim_id="claim-review-a",
            effect=effect,
            worker_id="test-worker",
            attempt_count=1,
        ),
    )


@pytest.mark.asyncio
async def test_review_effect_projects_exact_snapshot_and_encodes_completion() -> None:
    """One model decision sees only captured rows and returns deferred intent."""

    entries = (_entry(11, ledger_sequence=1), _entry(7, ledger_sequence=2))
    message_store = _MessageStore(
        {
            11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "first"},
            7: {"id": 7, "session_id": _BASE_SESSION_ID, "raw_text": "second"},
        }
    )
    reply_runner = _ReplyRunner(
        ReplyDecisionStageOutput(
            replied=True,
            target_message_ids=[11, 7],
            external_action_intents=(
                ExternalActionIntent(
                    kind=ExternalActionKind.SEND_REPLY,
                    tool_call_id="reply-call-a",
                    action_ordinal=0,
                    payload={"text": "hello", "quote_message_log_id": 11},
                ),
            ),
            reason="send_reply_tool",
            model_execution_id="model-review-a",
            prompt_signature="prompt-review-a",
        )
    )
    workflow = RunnerReviewWorkflow(
        projector=ActorReviewWorkflowContextProjector(message_store=message_store),
        reply_runner=reply_runner,
    )

    result = await ReviewWorkflowEffectHandler(
        ledger=_Ledger(entries),
        workflow=workflow,
    )(_context(_effect()))

    completion = ReviewCompletionResult.from_payload(result.payload["workflow_result"])
    assert message_store.requested_ids == [(11, 7)]
    assert len(reply_runner.inputs) == 1
    stage_input = reply_runner.inputs[0]
    assert stage_input.session_id == _KEY.session_id
    assert stage_input.instance_id == "instance-a"
    assert [item["id"] for item in stage_input.source_messages] == [11, 7]
    assert stage_input.metadata["candidate_message_ids"] == [11, 7]
    assert completion.enter_active_chat is False
    assert completion.consumed_message_log_ids == (11, 7)
    assert completion.next_review_outcome is not None
    assert completion.next_review_outcome.kind is ReviewNextReviewOutcomeKind.DEFAULTED
    assert len(completion.external_action_intents) == 1
    assert result.payload["model_execution_id"] == "model-review-a"
    assert result.payload["prompt_signature"] == "prompt-review-a"


@pytest.mark.asyncio
async def test_context_projector_rejects_message_log_from_another_transport_session() -> None:
    """The unscoped message store cannot inject a row from a different channel."""

    entries = (_entry(11, ledger_sequence=1),)
    projector = ActorReviewWorkflowContextProjector(
        message_store=_MessageStore(
            {
                11: {
                    "id": 11,
                    "session_id": "instance-a:group:another-room",
                    "raw_text": "wrong session",
                }
            }
        )
    )

    with pytest.raises(
        ActorReviewWorkflowContextError,
        match="message log session mismatch: 11",
    ):
        await projector.build_review_stage_input(_request(entries))


@pytest.mark.asyncio
async def test_review_workflow_fails_closed_for_model_failure_without_output() -> None:
    """A model failure raises before the workflow can return consumable IDs."""

    entries = (_entry(11, ledger_sequence=1),)
    workflow = RunnerReviewWorkflow(
        projector=ActorReviewWorkflowContextProjector(
            message_store=_MessageStore(
                {11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "hello"}}
            )
        ),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        ),
    )

    with pytest.raises(ActorReviewWorkflowError, match="decision failed"):
        await workflow.run_review(_request(entries))


@pytest.mark.asyncio
async def test_review_workflow_fails_closed_for_invalid_model_tool_decision() -> None:
    """A malformed external-action choice cannot silently consume reviewed input."""

    entries = (_entry(11, ledger_sequence=1),)
    workflow = RunnerReviewWorkflow(
        projector=ActorReviewWorkflowContextProjector(
            message_store=_MessageStore(
                {11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "hello"}}
            )
        ),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                reason="reply_external_action_invalid:send_reply:ValueError"
            )
        ),
    )

    with pytest.raises(ActorReviewWorkflowError, match="decision failed"):
        await workflow.run_review(_request(entries))


@pytest.mark.asyncio
async def test_review_workflow_rejects_unbound_poke_intents() -> None:
    """The first vertical slice cannot emit a poke without a sender binding."""

    entries = (_entry(11, ledger_sequence=1),)
    workflow = RunnerReviewWorkflow(
        projector=ActorReviewWorkflowContextProjector(
            message_store=_MessageStore(
                {11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "hello"}}
            )
        ),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                replied=True,
                target_message_ids=[11],
                external_action_intents=(
                    ExternalActionIntent(
                        kind=ExternalActionKind.SEND_POKE,
                        tool_call_id="poke-a",
                        action_ordinal=0,
                        payload={"user_id": "user-a"},
                    ),
                ),
                reason="send_poke_tool",
            )
        ),
    )

    with pytest.raises(ActorReviewWorkflowError, match="does not permit"):
        await workflow.run_review(_request(entries))


@pytest.mark.asyncio
async def test_review_workflow_skips_model_call_for_empty_captured_snapshot() -> None:
    """A due review with no unread input still settles to an idle default plan."""

    class _UnusedProjector:
        async def build_review_stage_input(
            self,
            request: ReviewWorkflowRequest,
        ) -> ReviewStageInput:
            raise AssertionError("empty review must not build a model stage")

    reply_runner = _ReplyRunner(ReplyDecisionStageOutput())
    output = await RunnerReviewWorkflow(
        projector=_UnusedProjector(),
        reply_runner=reply_runner,
    ).run_review(_request(()))

    assert reply_runner.inputs == []
    assert output.enter_active_chat is False
    assert output.consumed_message_log_ids == ()
    assert output.next_review_outcome is not None
    assert output.next_review_outcome.kind is ReviewNextReviewOutcomeKind.DEFAULTED
