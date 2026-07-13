"""Tests for the Actor v2 high-priority reply workflow boundary."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runtime.session_actor.active_reply_workflow import (
    ActorActiveReplyWorkflowError,
    RunnerActiveReplyWorkflow,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageLedgerEntry,
    MessagePriorityFlags,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowRequest,
    ActorWorkflowEffectInput,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput

_KEY = SessionKey("profile-a", "bot-a:instance-a:group:room-a")
_BASE_SESSION_ID = "instance-a:group:room-a"


@dataclass(slots=True)
class _Projector:
    """Controlled projection seam used to prove workflow input validation."""

    stage_input: ReviewStageInput
    requests: list[ActiveReplyWorkflowRequest] = field(default_factory=list)

    async def build_active_reply_stage_input(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ReviewStageInput:
        """Record the request and return the configured immutable stage."""

        self.requests.append(request)
        return self.stage_input


@dataclass(slots=True)
class _ReplyRunner:
    """Deterministic reply-decision runner with no tool execution surface."""

    output: ReplyDecisionStageOutput
    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Record a bounded model input and return the configured output."""

        self.inputs.append(stage_input)
        return self.output


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    ownership_generation: int = 1,
) -> MessageLedgerEntry:
    """Build one eligible high-priority ledger entry."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=ownership_generation,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id="user-a",
        instance_id="instance-a",
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=_BASE_SESSION_ID,
        platform="test",
        self_id="bot-a",
        response_profile="balanced",
        priority=MessagePriorityFlags(mention=True, should_wake_active_reply=True),
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
    *,
    message_log_ids: tuple[int, ...] | None = None,
) -> ActiveReplyWorkflowRequest:
    """Build a trusted active-reply request over the supplied ledger rows."""

    selected = message_log_ids or tuple(entry.message_log_id for entry in entries)
    return ActiveReplyWorkflowRequest(
        effect=ActorWorkflowEffectInput(
            key=_KEY,
            operation_id="active-reply-operation-a",
            effect_id="active-reply-effect-a",
            idempotency_key="active-reply-effect-a",
            source_event_id="message:11",
            ownership_generation=1,
            instance_id="instance-a",
            target_session_id=_BASE_SESSION_ID,
            input_watermark=20,
            input_ledger_sequence=2,
            ledger_entries=entries,
        ),
        message_log_ids=selected,
        response_profile="balanced",
        sender_id="user-a",
    )


def _stage_input(request: ActiveReplyWorkflowRequest) -> ReviewStageInput:
    """Build a valid stage projection for one exact active-reply request."""

    effect = request.effect
    message_log_ids = list(request.message_log_ids)
    return ReviewStageInput(
        session_id=effect.key.session_id,
        purpose="reply_decision",
        source_messages=[{"id": message_log_id} for message_log_id in message_log_ids],
        instance_id=effect.instance_id,
        metadata={
            "actor_v2": True,
            "operation_id": effect.operation_id,
            "effect_id": effect.effect_id,
            "ownership_generation": effect.ownership_generation,
            "input_watermark": effect.input_watermark,
            "input_ledger_sequence": effect.input_ledger_sequence,
            "target_session_id": effect.target_session_id,
            "ledger_message_log_ids": message_log_ids,
            "candidate_message_ids": message_log_ids,
            "response_profile": request.response_profile,
            "sender_id": request.sender_id,
        },
    )


@pytest.mark.asyncio
async def test_active_reply_workflow_consumes_exact_input_and_defers_bound_reply() -> None:
    """A valid model decision returns only actor-fenced consumption and intent."""

    request = _request((_entry(11, ledger_sequence=1),))
    reply_runner = _ReplyRunner(
        ReplyDecisionStageOutput(
            replied=True,
            target_message_ids=[11],
            external_action_intents=(
                ExternalActionIntent(
                    kind=ExternalActionKind.SEND_REPLY,
                    tool_call_id="reply-call-a",
                    action_ordinal=0,
                    payload={"text": "hello", "quote_message_log_id": 11},
                ),
            ),
            reason="send_reply_tool",
            model_execution_id="model-active-reply-a",
            prompt_signature="prompt-active-reply-a",
        )
    )
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=reply_runner,
    )

    output = await workflow.run_active_reply(request)

    assert len(reply_runner.inputs) == 1
    assert output.consumed_message_log_ids == (11,)
    assert output.external_action_intents[0].payload == {
        "text": "hello",
        "quote_message_log_id": 11,
    }
    assert output.model_execution_id == "model-active-reply-a"
    assert output.prompt_signature == "prompt-active-reply-a"


@pytest.mark.asyncio
async def test_active_reply_workflow_uses_ledger_order_not_request_id_order() -> None:
    """Selection order cannot change a prompt or completion's durable ordering."""

    request = _request(
        (_entry(11, ledger_sequence=1), _entry(10, ledger_sequence=2)),
        message_log_ids=(10, 11),
    )
    stage_input = _stage_input(request)
    stage_input.source_messages[:] = [{"id": 11}, {"id": 10}]
    stage_input.metadata["ledger_message_log_ids"] = [11, 10]
    stage_input.metadata["candidate_message_ids"] = [11, 10]
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(stage_input),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                target_message_ids=[11, 10],
                reason="no_reply_tool",
            )
        ),
    )

    output = await workflow.run_active_reply(request)

    assert output.consumed_message_log_ids == (11, 10)


@pytest.mark.asyncio
async def test_active_reply_workflow_rejects_widened_projector_input_before_model_call() -> None:
    """A projector cannot add a message that the active-reply effect omitted."""

    request = _request((_entry(11, ledger_sequence=1),))
    widened = _stage_input(request)
    widened.source_messages[:] = [{"id": 12}]
    widened.metadata["ledger_message_log_ids"] = [12]
    widened.metadata["candidate_message_ids"] = [12]
    reply_runner = _ReplyRunner(ReplyDecisionStageOutput())
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(widened),
        reply_runner=reply_runner,
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="captured message selection"):
        await workflow.run_active_reply(request)

    assert reply_runner.inputs == []


@pytest.mark.asyncio
async def test_active_reply_workflow_rejects_unbound_platform_quote() -> None:
    """A model cannot make a reply target by submitting a raw platform ID."""

    request = _request((_entry(11, ledger_sequence=1),))
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                replied=True,
                target_message_ids=[11],
                external_action_intents=(
                    ExternalActionIntent(
                        kind=ExternalActionKind.SEND_REPLY,
                        tool_call_id="reply-call-a",
                        action_ordinal=0,
                        payload={"text": "hello", "quote_message_id": "platform-11"},
                    ),
                ),
                reason="send_reply_tool",
            )
        ),
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="unbound platform quote"):
        await workflow.run_active_reply(request)


@pytest.mark.asyncio
async def test_active_reply_workflow_rejects_unbound_poke_intent() -> None:
    """The first active-reply slice does not permit model-selected pokes."""

    request = _request((_entry(11, ledger_sequence=1),))
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                replied=True,
                target_message_ids=[11],
                external_action_intents=(
                    ExternalActionIntent(
                        kind=ExternalActionKind.SEND_POKE,
                        tool_call_id="poke-call-a",
                        action_ordinal=0,
                        payload={"user_id": "user-a"},
                    ),
                ),
                reason="send_poke_tool",
            )
        ),
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="does not permit"):
        await workflow.run_active_reply(request)


@pytest.mark.asyncio
async def test_active_reply_workflow_fails_closed_for_model_failure() -> None:
    """A failed model result never returns consumable active-reply input."""

    request = _request((_entry(11, ledger_sequence=1),))
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        ),
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="decision failed"):
        await workflow.run_active_reply(request)


@pytest.mark.asyncio
async def test_active_reply_workflow_rejects_toolless_no_reply_output() -> None:
    """A bare-model no-reply result cannot consume high-priority input."""

    request = _request((_entry(11, ledger_sequence=1),))
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                target_message_ids=[11],
                reason="tool_call_plan_toolless",
            )
        ),
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="no-reply outcome"):
        await workflow.run_active_reply(request)


@pytest.mark.asyncio
async def test_active_reply_workflow_rejects_a_second_reply_intent() -> None:
    """One high-priority decision cannot produce an unbounded reply sequence."""

    request = _request((_entry(11, ledger_sequence=1),))
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(_stage_input(request)),
        reply_runner=_ReplyRunner(
            ReplyDecisionStageOutput(
                replied=True,
                target_message_ids=[11],
                external_action_intents=(
                    ExternalActionIntent(
                        kind=ExternalActionKind.SEND_REPLY,
                        tool_call_id="reply-call-a",
                        action_ordinal=0,
                        payload={"text": "first", "quote_message_log_id": 11},
                    ),
                    ExternalActionIntent(
                        kind=ExternalActionKind.SEND_REPLY,
                        tool_call_id="reply-call-b",
                        action_ordinal=1,
                        payload={"text": "second", "quote_message_log_id": 11},
                    ),
                ),
                reason="send_reply_tool:2",
            )
        ),
    )

    with pytest.raises(ActorActiveReplyWorkflowError, match="at most one"):
        await workflow.run_active_reply(request)


@pytest.mark.asyncio
async def test_active_reply_workflow_skips_model_call_for_empty_snapshot() -> None:
    """A defensive empty request does not invoke a model or consume anything."""

    request = _request(())
    reply_runner = _ReplyRunner(ReplyDecisionStageOutput())
    workflow = RunnerActiveReplyWorkflow(
        projector=_Projector(
            ReviewStageInput(
                session_id=_KEY.session_id,
                purpose="reply_decision",
                source_messages=[],
                instance_id="instance-a",
            )
        ),
        reply_runner=reply_runner,
    )

    output = await workflow.run_active_reply(request)

    assert reply_runner.inputs == []
    assert output.consumed_message_log_ids == ()
