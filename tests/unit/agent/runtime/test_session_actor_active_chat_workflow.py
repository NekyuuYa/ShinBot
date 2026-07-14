"""Tests for Actor v3 Active Chat workflow model boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from shinbot.agent.runners.templates import RunnerTemplateConfig, ToolCallPlanResult
from shinbot.agent.runners.templates.structured_output import StructuredOutputRun
from shinbot.agent.runtime.session_actor.active_chat_workflow import (
    ActorActiveChatWorkflowError,
    RunnerActiveChatBootstrapWorkflow,
    RunnerActiveChatRoundWorkflow,
    build_actor_active_chat_round_plan_runner,
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
    ActiveChatBootstrapWorkflowRequest,
    ActiveChatRoundWorkflowOutput,
    ActiveChatRoundWorkflowRequest,
    ActorWorkflowEffectInput,
    WorkflowEffectAdapterError,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapDisposition,
    ActiveChatRoundOutcome,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.prompt_engine import PromptRegistry, PromptStage
from shinbot.agent.workflows.active_chat.prompt_registration import (
    ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS,
)

_KEY = SessionKey("profile-a", "bot-a:instance-a:group:room-a")
_INSTANCE_ID = "instance-a"
_BASE_SESSION_ID = "instance-a:group:room-a"


@dataclass(slots=True)
class _BootstrapProjector:
    """Controlled bootstrap projection seam that records actor requests."""

    stage_input: ReviewStageInput
    requests: list[ActiveChatBootstrapWorkflowRequest] = field(default_factory=list)

    async def build_active_chat_bootstrap_stage_input(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ReviewStageInput:
        """Return the configured stage input without reaching a live context."""

        self.requests.append(request)
        return self.stage_input


@dataclass(slots=True)
class _BootstrapRunner:
    """Structured-output fake used to verify bootstrap provenance handling."""

    output: StructuredOutputRun
    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run_with_provenance(self, stage_input: ReviewStageInput) -> StructuredOutputRun:
        """Record the fully fenced input and return one model result."""

        self.inputs.append(stage_input)
        return self.output


@dataclass(slots=True)
class _RoundProjector:
    """Controlled round projection seam that records actor requests."""

    stage_input: ReviewStageInput
    requests: list[ActiveChatRoundWorkflowRequest] = field(default_factory=list)

    async def build_active_chat_round_stage_input(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ReviewStageInput:
        """Return the configured immutable stage input."""

        self.requests.append(request)
        return self.stage_input


@dataclass(slots=True)
class _PlanRunner:
    """Planner fake that never executes the model-selected action."""

    output: ToolCallPlanResult
    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> ToolCallPlanResult:
        """Record one validated input and return a raw tool-call plan."""

        self.inputs.append(stage_input)
        return self.output


def _entry(message_log_id: int, *, ledger_sequence: int) -> MessageLedgerEntry:
    """Build one eligible unread entry in durable ledger order."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=1,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id=f"user:{message_log_id}",
        instance_id=_INSTANCE_ID,
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=_BASE_SESSION_ID,
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


def _effect(entries: tuple[MessageLedgerEntry, ...]) -> ActorWorkflowEffectInput:
    """Build one frozen Actor effect whose entries are in ledger order."""

    return ActorWorkflowEffectInput(
        key=_KEY,
        operation_id="active-chat-operation-a",
        effect_id="active-chat-effect-a",
        idempotency_key="active-chat-idempotency-a",
        source_event_id="active-chat-due-a",
        ownership_generation=1,
        instance_id=_INSTANCE_ID,
        target_session_id=_BASE_SESSION_ID,
        input_watermark=100,
        input_ledger_sequence=100,
        ledger_entries=entries,
    )


def _bootstrap_request(
    handoff_message_log_ids: tuple[int, ...] = (21,),
) -> ActiveChatBootstrapWorkflowRequest:
    """Build an immutable bootstrap request over a review-owned handoff."""

    return ActiveChatBootstrapWorkflowRequest(
        effect=_effect(()),
        active_epoch=3,
        handoff_operation_id="review-operation-a",
        handoff_message_log_ids=handoff_message_log_ids,
    )


def _round_request(
    entries: tuple[MessageLedgerEntry, ...] = (
        _entry(21, ledger_sequence=1),
        _entry(10, ledger_sequence=2),
    ),
) -> ActiveChatRoundWorkflowRequest:
    """Build one Active Chat round request with a deliberately stable fence."""

    return ActiveChatRoundWorkflowRequest(
        effect=_effect(entries),
        active_epoch=3,
        round_schedule_id="round-schedule-a",
        message_log_ids=tuple(entry.message_log_id for entry in entries),
        interest_value=12.0,
        bootstrap_disposition=ActiveChatBootstrapDisposition.ENGAGED.value,
    )


def _bootstrap_stage_input(
    request: ActiveChatBootstrapWorkflowRequest,
) -> ReviewStageInput:
    """Build a stage input that mirrors the bootstrap projector contract."""

    selected_ids = list(request.handoff_message_log_ids)
    return ReviewStageInput(
        session_id=_KEY.session_id,
        purpose="active_chat_bootstrap",
        source_messages=[{"id": message_log_id} for message_log_id in selected_ids],
        instance_id=_INSTANCE_ID,
        metadata={
            "purpose": "active_chat_bootstrap",
            "actor_v2": True,
            "active_chat_v3": True,
            "operation_id": request.effect.operation_id,
            "effect_id": request.effect.effect_id,
            "ownership_generation": request.effect.ownership_generation,
            "input_watermark": request.effect.input_watermark,
            "input_ledger_sequence": request.effect.input_ledger_sequence,
            "target_session_id": request.effect.target_session_id,
            "ledger_message_log_ids": selected_ids,
            "candidate_message_ids": selected_ids,
            "active_epoch": request.active_epoch,
            "handoff_operation_id": request.handoff_operation_id,
            "handoff_message_log_ids": selected_ids,
        },
    )


def _round_stage_input(request: ActiveChatRoundWorkflowRequest) -> ReviewStageInput:
    """Build an exact actor-native round stage input in durable ledger order."""

    selected_ids = list(request.effect.message_log_ids)
    return ReviewStageInput(
        session_id=_KEY.session_id,
        purpose="active_chat_round",
        source_messages=[{"id": message_log_id} for message_log_id in selected_ids],
        instance_id=_INSTANCE_ID,
        metadata={
            "purpose": "active_chat_round",
            "actor_v2": True,
            "active_chat_v3": True,
            "operation_id": request.effect.operation_id,
            "effect_id": request.effect.effect_id,
            "ownership_generation": request.effect.ownership_generation,
            "input_watermark": request.effect.input_watermark,
            "input_ledger_sequence": request.effect.input_ledger_sequence,
            "target_session_id": request.effect.target_session_id,
            "ledger_message_log_ids": selected_ids,
            "candidate_message_ids": selected_ids,
            "active_epoch": request.active_epoch,
            "round_schedule_id": request.round_schedule_id,
            "interest_value": request.interest_value,
            "active_chat_interest_value": request.interest_value,
            "bootstrap_disposition": request.bootstrap_disposition,
            "message_log_ids": selected_ids,
        },
    )


def _tool_call(name: str, arguments: object, *, call_id: str = "call-a") -> dict[str, object]:
    """Build one raw planner tool call in the Chat Completions shape."""

    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def test_round_plan_runner_uses_only_its_actor_v3_prompt_components() -> None:
    """Legacy configured constraints cannot broaden the native round grammar."""

    routing = RunnerTemplateConfig(
        component_ids_by_stage={
            PromptStage.CONSTRAINTS: ["active_chat.fast_mode.constraints"],
        }
    )
    runner = build_actor_active_chat_round_plan_runner(
        object(),
        prompt_registry=PromptRegistry(),
        tool_manager=object(),
        config=routing,
    )

    assert runner._config.component_ids_by_stage == {}  # noqa: SLF001
    assert runner._config.builtin_component_ids == (  # noqa: SLF001
        ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS["round"]
    )


@pytest.mark.asyncio
async def test_bootstrap_workflow_returns_disposition_with_model_provenance() -> None:
    """Bootstrap carries only its discrete decision and runner provenance."""

    request = _bootstrap_request()
    bootstrap_runner = _BootstrapRunner(
        StructuredOutputRun(
            payload={"disposition": "engaged", "reason": "conversation active"},
            model_execution_id="model-bootstrap-a",
            prompt_signature="prompt-bootstrap-a",
        )
    )
    workflow = RunnerActiveChatBootstrapWorkflow(
        projector=_BootstrapProjector(_bootstrap_stage_input(request)),
        bootstrap_runner=bootstrap_runner,  # type: ignore[arg-type]
    )

    output = await workflow.run_active_chat_bootstrap(request)

    assert output.disposition is ActiveChatBootstrapDisposition.ENGAGED
    assert output.reason == "conversation active"
    assert output.model_execution_id == "model-bootstrap-a"
    assert output.prompt_signature == "prompt-bootstrap-a"
    assert len(bootstrap_runner.inputs) == 1


@pytest.mark.asyncio
async def test_round_workflow_rejects_widened_projector_input_before_model_call() -> None:
    """A projector cannot add a message that the round effect did not select."""

    request = _round_request((_entry(21, ledger_sequence=1),))
    widened = _round_stage_input(request)
    widened.source_messages[:] = [{"id": 99}]
    widened.metadata["ledger_message_log_ids"] = [99]
    widened.metadata["candidate_message_ids"] = [99]
    widened.metadata["message_log_ids"] = [99]
    plan_runner = _PlanRunner(ToolCallPlanResult())
    workflow = RunnerActiveChatRoundWorkflow(
        projector=_RoundProjector(widened),
        plan_runner=plan_runner,  # type: ignore[arg-type]
    )

    with pytest.raises(
        ActorActiveChatWorkflowError,
        match="captured message selection",
    ):
        await workflow.run_active_chat_round(request)

    assert plan_runner.inputs == []


@pytest.mark.asyncio
async def test_round_no_reply_consumes_exact_ledger_selection_with_provenance() -> None:
    """A no-reply decision consumes exactly the frozen round selection."""

    request = _round_request()
    plan_runner = _PlanRunner(
        ToolCallPlanResult(
            tool_calls=[
                _tool_call(
                    "no_reply",
                    {"intensity": "normal", "reason": "conversation paused"},
                )
            ],
            execution_id="model-round-no-reply-a",
            metadata={"prompt_signature": "prompt-round-no-reply-a"},
            reason="tool_call_plan",
        )
    )
    workflow = RunnerActiveChatRoundWorkflow(
        projector=_RoundProjector(_round_stage_input(request)),
        plan_runner=plan_runner,  # type: ignore[arg-type]
    )

    output = await workflow.run_active_chat_round(request)

    assert output.outcome is ActiveChatRoundOutcome.CONTINUE
    assert output.consumed_message_log_ids == (21, 10)
    assert output.external_action_intents == ()
    assert output.interest_delta == -5.0
    assert output.reason == "conversation paused"
    assert output.model_execution_id == "model-round-no-reply-a"
    assert output.prompt_signature == "prompt-round-no-reply-a"


@pytest.mark.asyncio
async def test_round_bound_reply_consumes_exact_selection_and_defers_one_intent() -> None:
    """A valid reply remains a deferred durable-ID-bound action candidate."""

    request = _round_request()
    plan_runner = _PlanRunner(
        ToolCallPlanResult(
            tool_calls=[
                _tool_call(
                    "send_reply",
                    {
                        "text": "I am here.",
                        "quote_message_log_id": 10,
                        "intensity": "engaged",
                        "reason": "direct response",
                    },
                )
            ],
            execution_id="model-round-reply-a",
            metadata={"prompt_signature": "prompt-round-reply-a"},
            reason="tool_call_plan",
        )
    )
    workflow = RunnerActiveChatRoundWorkflow(
        projector=_RoundProjector(_round_stage_input(request)),
        plan_runner=plan_runner,  # type: ignore[arg-type]
    )

    output = await workflow.run_active_chat_round(request)

    assert output.outcome is ActiveChatRoundOutcome.CONTINUE
    assert output.consumed_message_log_ids == (21, 10)
    assert output.interest_delta == 10.0
    assert output.model_execution_id == "model-round-reply-a"
    assert output.prompt_signature == "prompt-round-reply-a"
    assert output.external_action_intents == (
        ExternalActionIntent(
            kind=ExternalActionKind.SEND_REPLY,
            tool_call_id="call-a",
            action_ordinal=0,
            payload={"text": "I am here.", "quote_message_log_id": 10},
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool_calls",
    [
        [_tool_call("send_reply", {"text": "bad", "quote_message_id": "p-21"})],
        [
            _tool_call(
                "send_reaction",
                {"emoji_id": "wave", "message_id": "p-21", "action": "add"},
            )
        ],
        [_tool_call("send_poke", {"user_id": "user-a"})],
        [_tool_call("no_reply", "[]")],
        [_tool_call("no_reply", "{invalid-json")],
        [
            _tool_call("no_reply", {"intensity": "normal"}, call_id="call-a"),
            _tool_call(
                "send_reply",
                {"text": "second", "quote_message_log_id": 21},
                call_id="call-b",
            ),
        ],
    ],
)
async def test_round_invalid_visible_actions_become_non_executable_retries(
    tool_calls: list[dict[str, object]],
) -> None:
    """Platform IDs, pokes, and multiple actions cannot escape as intents."""

    request = _round_request()
    plan_runner = _PlanRunner(
        ToolCallPlanResult(
            tool_calls=tool_calls,
            execution_id="model-round-invalid-a",
            metadata={"prompt_signature": "prompt-round-invalid-a"},
            reason="tool_call_plan",
        )
    )
    workflow = RunnerActiveChatRoundWorkflow(
        projector=_RoundProjector(_round_stage_input(request)),
        plan_runner=plan_runner,  # type: ignore[arg-type]
    )

    output = await workflow.run_active_chat_round(request)

    assert output.outcome is ActiveChatRoundOutcome.RETRY
    assert output.interest_delta == 0.0
    assert output.consumed_message_log_ids == ()
    assert output.external_action_intents == ()
    assert output.model_execution_id == "model-round-invalid-a"
    assert output.prompt_signature == "prompt-round-invalid-a"


def test_retry_with_action_is_rejected_before_an_executable_completion_exists() -> None:
    """The typed output contract itself forbids retry outcomes with actions."""

    with pytest.raises(
        WorkflowEffectAdapterError,
        match="retry active chat round cannot consume messages or propose actions",
    ):
        ActiveChatRoundWorkflowOutput(
            outcome=ActiveChatRoundOutcome.RETRY,
            interest_delta=0.0,
            reason="bad retry",
            external_action_intents=(
                ExternalActionIntent(
                    kind=ExternalActionKind.SEND_REPLY,
                    tool_call_id="call-a",
                    action_ordinal=0,
                    payload={"text": "must not send", "quote_message_log_id": 21},
                ),
            ),
        )
