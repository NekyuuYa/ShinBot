"""Unit tests for actor-owned review and active-reply workflow adapters."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    EffectExecutionContext,
    EffectHandlerRegistry,
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
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowEffectHandler,
    ActiveReplyWorkflowOutput,
    ActiveReplyWorkflowRequest,
    ReviewWorkflowEffectHandler,
    ReviewWorkflowOutput,
    ReviewWorkflowRequest,
    ReviewWorkflowWindowOutput,
    WorkflowEffectAdapterError,
    operation_global_review_proposal_id,
    register_actor_workflow_effect_handlers,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveReplyCompletionResult,
    ReviewCompletionResult,
    ReviewNextReviewOutcome,
    ReviewNextReviewOutcomeKind,
)
from shinbot.agent.workflows.chat_actions.intents import ExternalActionToolMode

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


class _UnusedEffectStore:
    """Minimal store because these adapter tests do not renew effect leases."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Return the current claim if a test accidentally requests renewal."""

        return claim


@dataclass(slots=True)
class _Ledger:
    """Recorded captured-ledger fake for workflow handler tests."""

    entries: tuple[MessageLedgerEntry, ...]
    calls: list[tuple[SessionKey, int, int]] = field(default_factory=list)

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Record the trusted actor boundary and return the configured rows."""

        self.calls.append((key, input_watermark, input_ledger_sequence))
        return self.entries


@dataclass(slots=True)
class _ActiveReplyWorkflow:
    """Pure active-reply workflow fake that only records its request."""

    output: ActiveReplyWorkflowOutput
    requests: list[ActiveReplyWorkflowRequest] = field(default_factory=list)

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Return the configured pure model result."""

        self.requests.append(request)
        return self.output


@dataclass(slots=True)
class _ReviewWorkflow:
    """Pure review workflow fake that only records its request."""

    output: ReviewWorkflowOutput
    requests: list[ReviewWorkflowRequest] = field(default_factory=list)

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Return the configured pure model result."""

        self.requests.append(request)
        return self.output


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    is_mentioned: bool = False,
) -> MessageLedgerEntry:
    """Build one unread actor-ledger row for a deterministic workflow snapshot."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=1,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id="user-a",
        instance_id="instance-a",
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        platform="test",
        self_id="bot-a",
        is_mentioned=is_mentioned,
        priority=MessagePriorityFlags(
            mention=is_mentioned,
            should_wake_active_reply=is_mentioned,
        ),
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


def _effect(
    *,
    kind: str,
    operation_id: str,
    payload: dict[str, object],
) -> DurableEffectEnvelope:
    """Build one durable actor workflow effect with trusted payload fields."""

    contract = builtin_effect_contract(kind)
    effect_id = f"effect:{operation_id}"
    idempotency_key = f"idempotency:{operation_id}"
    source_event_id = f"source:{operation_id}"
    return DurableEffectEnvelope(
        effect_id=effect_id,
        key=_KEY,
        kind=kind,
        idempotency_key=idempotency_key,
        ownership_generation=1,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload={
            "operation_id": operation_id,
            "effect_id": effect_id,
            "effect_kind": kind,
            "idempotency_key": idempotency_key,
            "source_event_id": source_event_id,
            "ownership_generation": 1,
            "input_watermark": 20,
            "input_ledger_sequence": 2,
            "instance_id": "instance-a",
            "target_session_id": "instance-a:group:room-a",
            **payload,
        },
        source_event_id=source_event_id,
        operation_id=operation_id,
        trace_id="trace-a",
    )


def _context(effect: DurableEffectEnvelope) -> EffectExecutionContext:
    """Create a leased effect context without involving an effect executor."""

    return EffectExecutionContext(
        _UnusedEffectStore(),  # type: ignore[arg-type]
        ClaimedEffect(
            claim_id=f"claim:{effect.effect_id}",
            effect=effect,
            worker_id="test-worker",
            attempt_count=1,
        ),
    )


def _reply_intent(tool_call_id: str, *, ordinal: int = 0) -> ExternalActionIntent:
    """Build one normalized deferred send-reply model proposal."""

    return ExternalActionIntent(
        kind=ExternalActionKind.SEND_REPLY,
        tool_call_id=tool_call_id,
        action_ordinal=ordinal,
        payload={"text": "hello"},
    )


@pytest.mark.asyncio
async def test_active_reply_handler_uses_captured_ledger_and_encodes_nested_result() -> None:
    """Active reply can return intents but cannot alter actor-owned context."""

    effect = _effect(
        kind="run_active_reply_workflow",
        operation_id="active-reply-a",
        payload={
            "message_log_ids": [10],
            "response_profile": "balanced",
            "sender_id": "user-a",
        },
    )
    ledger = _Ledger((_entry(10, ledger_sequence=1, is_mentioned=True), _entry(11, ledger_sequence=2)))
    workflow = _ActiveReplyWorkflow(
        ActiveReplyWorkflowOutput(
            consumed_message_log_ids=(10,),
            external_action_intents=(_reply_intent("model-call-a"),),
        )
    )

    result = await ActiveReplyWorkflowEffectHandler(
        ledger=ledger,
        workflow=workflow,
    )(_context(effect))

    assert ledger.calls == [(_KEY, 20, 2)]
    assert len(workflow.requests) == 1
    request = workflow.requests[0]
    assert request.external_action_mode is ExternalActionToolMode.COLLECT_INTENTS
    assert request.effect.instance_id == "instance-a"
    assert request.effect.target_session_id == "instance-a:group:room-a"
    assert request.message_log_ids == (10,)
    assert request.effect.message_log_ids == (10,)
    completion = ActiveReplyCompletionResult.from_payload(
        result.payload["workflow_result"]
    )
    assert completion.consumed_message_log_ids == (10,)
    assert [intent.tool_call_id for intent in completion.external_action_intents] == [
        "model-call-a"
    ]
    assert set(result.payload) == {"workflow_result"}


@pytest.mark.asyncio
async def test_active_reply_handler_rejects_untrusted_target_before_ledger_or_workflow() -> None:
    """A persisted target mismatch cannot redirect a workflow model invocation."""

    effect = _effect(
        kind="run_active_reply_workflow",
        operation_id="active-reply-a",
        payload={
            "message_log_ids": [10],
            "target_session_id": "another-session",
        },
    )
    ledger = _Ledger((_entry(10, ledger_sequence=1, is_mentioned=True),))
    workflow = _ActiveReplyWorkflow(ActiveReplyWorkflowOutput())

    with pytest.raises(WorkflowEffectAdapterError, match="target_session_id"):
        await ActiveReplyWorkflowEffectHandler(
            ledger=ledger,
            workflow=workflow,
        )(_context(effect))

    assert ledger.calls == []
    assert workflow.requests == []


@pytest.mark.asyncio
async def test_active_reply_handler_rejects_late_ledger_rows_before_model_work() -> None:
    """A faulty ledger port cannot expose data after the captured sequence fence."""

    effect = _effect(
        kind="run_active_reply_workflow",
        operation_id="active-reply-a",
        payload={"message_log_ids": [10]},
    )
    ledger = _Ledger(
        (
            _entry(10, ledger_sequence=1, is_mentioned=True),
            _entry(9, ledger_sequence=3),
        )
    )
    workflow = _ActiveReplyWorkflow(ActiveReplyWorkflowOutput())

    with pytest.raises(WorkflowEffectAdapterError, match="input_ledger_sequence"):
        await ActiveReplyWorkflowEffectHandler(
            ledger=ledger,
            workflow=workflow,
        )(_context(effect))

    assert len(ledger.calls) == 1
    assert workflow.requests == []


@pytest.mark.asyncio
async def test_review_handler_makes_window_proposals_operation_global() -> None:
    """Repeated local model ids from windows become unique global action slots."""

    operation_id = "review-a"
    effect = _effect(
        kind="run_review_workflow",
        operation_id=operation_id,
        payload={
            "plan_id": "plan-a",
            "plan_revision": 3,
            "review_plan": {
                "plan_id": "plan-a",
                "plan_revision": 3,
                "reason": "timer_due",
            },
        },
    )
    ledger = _Ledger((_entry(7, ledger_sequence=1), _entry(8, ledger_sequence=2)))
    workflow = _ReviewWorkflow(
        ReviewWorkflowOutput(
            enter_active_chat=False,
            next_review_outcome=ReviewNextReviewOutcome(
                kind=ReviewNextReviewOutcomeKind.DEFAULTED,
                applied_delay_seconds=60.0,
                reason="review_finished",
                fallback_reason="default_policy",
            ),
            reply_windows=(
                ReviewWorkflowWindowOutput(
                    window_id="candidate:7",
                    consumed_message_log_ids=(7,),
                    external_action_intents=(_reply_intent("model-call"),),
                ),
                ReviewWorkflowWindowOutput(
                    window_id="candidate:8",
                    consumed_message_log_ids=(8,),
                    external_action_intents=(_reply_intent("model-call"),),
                ),
            ),
        )
    )

    result = await ReviewWorkflowEffectHandler(
        ledger=ledger,
        workflow=workflow,
    )(_context(effect))

    assert ledger.calls == [(_KEY, 20, 2)]
    assert len(workflow.requests) == 1
    request = workflow.requests[0]
    assert request.external_action_mode is ExternalActionToolMode.COLLECT_INTENTS
    assert request.effect.instance_id == "instance-a"
    assert request.effect.target_session_id == "instance-a:group:room-a"
    assert request.effect.message_log_ids == (7, 8)
    completion = ReviewCompletionResult.from_payload(result.payload["workflow_result"])
    assert completion.enter_active_chat is False
    assert completion.consumed_message_log_ids == (7, 8)
    assert [intent.action_ordinal for intent in completion.external_action_intents] == [0, 1]
    assert [intent.tool_call_id for intent in completion.external_action_intents] == [
        operation_global_review_proposal_id(
            operation_id=operation_id,
            window_id="candidate:7",
            local_proposal_id="model-call",
        ),
        operation_global_review_proposal_id(
            operation_id=operation_id,
            window_id="candidate:8",
            local_proposal_id="model-call",
        ),
    ]


@pytest.mark.asyncio
async def test_review_handler_rejects_window_consumption_outside_captured_input() -> None:
    """Review completion cannot consume a message it did not receive as input."""

    effect = _effect(
        kind="run_review_workflow",
        operation_id="review-a",
        payload={
            "plan_id": "plan-a",
            "plan_revision": 1,
            "review_plan": {"plan_id": "plan-a", "plan_revision": 1},
        },
    )
    ledger = _Ledger((_entry(7, ledger_sequence=1),))
    workflow = _ReviewWorkflow(
        ReviewWorkflowOutput(
            enter_active_chat=True,
            next_review_outcome=None,
            reply_windows=(
                ReviewWorkflowWindowOutput(
                    window_id="candidate:8",
                    consumed_message_log_ids=(8,),
                ),
            ),
        )
    )

    with pytest.raises(WorkflowEffectAdapterError, match="outside its captured ledger"):
        await ReviewWorkflowEffectHandler(
            ledger=ledger,
            workflow=workflow,
        )(_context(effect))


def test_explicit_registration_uses_builtin_actor_workflow_contracts() -> None:
    """Activation can register actor workflow handlers without starting runtime work."""

    ledger = _Ledger(())
    active_workflow = _ActiveReplyWorkflow(ActiveReplyWorkflowOutput())
    review_workflow = _ReviewWorkflow(
        ReviewWorkflowOutput(
            enter_active_chat=True,
            next_review_outcome=None,
        )
    )
    registry = EffectHandlerRegistry()

    active_handler, review_handler = register_actor_workflow_effect_handlers(
        registry,
        ledger=ledger,
        active_reply_workflow=active_workflow,
        review_workflow=review_workflow,
    )

    active_contract, registered_active_handler = registry.resolve(
        "run_active_reply_workflow"
    )
    review_contract, registered_review_handler = registry.resolve("run_review_workflow")
    assert active_contract == builtin_effect_contract("run_active_reply_workflow")
    assert review_contract == builtin_effect_contract("run_review_workflow")
    assert registered_active_handler is active_handler
    assert registered_review_handler is review_handler
