"""Actor-native high-priority reply workflow with deferred actions only.

The legacy active-reply path borrows the mutable active-chat coordinator.  It
can merge new input, schedule repair work, and execute visible tools before a
durable actor transition accepts the decision.  This module deliberately uses
the narrow reply-decision stage instead: one effect-captured message selection
becomes one model decision whose visible work is represented only by deferred
external-action intents.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Protocol

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runners.review_reply.runner import (
    LLMReplyDecisionStageRunner,
    ReplyDecisionStageRunner,
)
from shinbot.agent.runners.templates import RunnerTemplateConfig
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.runtime.session_actor.review_workflow import (
    build_actor_review_reply_decision_runner,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowOutput,
    ActiveReplyWorkflowPort,
    ActiveReplyWorkflowRequest,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.workflows.action_mode import ExternalActionToolMode

_REPLY_STAGE_PURPOSE = "reply_decision"


class ActorActiveReplyWorkflowError(RuntimeError):
    """Raised when a high-priority reply cannot safely reach completion."""


class ActorActiveReplyWorkflowStageInputProjector(Protocol):
    """Build one exact reply-decision input from an Actor effect snapshot."""

    async def build_active_reply_stage_input(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ReviewStageInput:
        """Return an immutable-boundary reply-decision stage input."""


class RunnerActiveReplyWorkflow(ActiveReplyWorkflowPort):
    """Run one safe reply-decision stage over one actor-fenced selection."""

    def __init__(
        self,
        *,
        projector: ActorActiveReplyWorkflowStageInputProjector,
        reply_runner: ReplyDecisionStageRunner,
    ) -> None:
        self._projector = projector
        self._reply_runner = reply_runner

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Return only accepted consumption and deferred visible-action intents."""

        if not isinstance(request, ActiveReplyWorkflowRequest):
            raise TypeError("request must be ActiveReplyWorkflowRequest")
        if request.external_action_mode is not ExternalActionToolMode.COLLECT_INTENTS:
            raise ActorActiveReplyWorkflowError(
                "actor active reply workflow must collect external action intents"
            )
        selected_ids = _ledger_ordered_selected_message_log_ids(request)
        if not selected_ids:
            return ActiveReplyWorkflowOutput()

        stage_input = await self._projector.build_active_reply_stage_input(request)
        self._validate_stage_input(
            stage_input,
            request=request,
            selected_message_log_ids=selected_ids,
        )
        stage_output = await self._reply_runner.run(stage_input)
        if not isinstance(stage_output, ReplyDecisionStageOutput):
            raise TypeError("actor active reply runner returned an invalid output type")
        self._validate_reply_output(stage_output, captured_ids=selected_ids)
        return ActiveReplyWorkflowOutput(
            consumed_message_log_ids=selected_ids,
            external_action_intents=stage_output.external_action_intents,
            model_execution_id=stage_output.model_execution_id,
            prompt_signature=stage_output.prompt_signature,
        )

    @staticmethod
    def _validate_stage_input(
        stage_input: object,
        *,
        request: ActiveReplyWorkflowRequest,
        selected_message_log_ids: tuple[int, ...],
    ) -> None:
        """Prove that a projector did not widen or retarget model-visible input."""

        if not isinstance(stage_input, ReviewStageInput):
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector returned an invalid stage input"
            )
        effect = request.effect
        if stage_input.session_id != effect.key.session_id:
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector changed the actor session id"
            )
        if stage_input.instance_id != effect.instance_id:
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector changed the adapter instance id"
            )
        if stage_input.purpose != _REPLY_STAGE_PURPOSE:
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector changed the stage purpose"
            )
        expected_ids = selected_message_log_ids
        if _stage_message_log_ids(stage_input.source_messages) != expected_ids:
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector changed the captured message selection"
            )
        for metadata_key in ("ledger_message_log_ids", "candidate_message_ids"):
            if _metadata_message_log_ids(stage_input.metadata, metadata_key) != expected_ids:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply projector changed " + metadata_key
                )
        if stage_input.metadata.get("actor_v2") is not True:
            raise ActorActiveReplyWorkflowError(
                "actor active reply projector omitted actor_v2 provenance"
            )
        expected_metadata = {
            "operation_id": effect.operation_id,
            "effect_id": effect.effect_id,
            "ownership_generation": effect.ownership_generation,
            "input_watermark": effect.input_watermark,
            "input_ledger_sequence": effect.input_ledger_sequence,
            "target_session_id": effect.target_session_id,
            "response_profile": request.response_profile,
            "sender_id": request.sender_id,
        }
        for key, expected in expected_metadata.items():
            if stage_input.metadata.get(key) != expected:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply projector changed " + key
                )

    @staticmethod
    def _validate_reply_output(
        output: ReplyDecisionStageOutput,
        *,
        captured_ids: tuple[int, ...],
    ) -> None:
        """Reject a model output that cannot become receipt-fenced work."""

        if output.consumption_deferred:
            raise ActorActiveReplyWorkflowError(
                "actor active reply output cannot defer ledger consumption"
            )
        if _reply_output_requires_failure(output.reason):
            raise ActorActiveReplyWorkflowError(
                "actor active reply decision failed: " + str(output.reason)
            )
        if output.reply_message_id is not None or output.reply_message_ids:
            raise ActorActiveReplyWorkflowError(
                "actor active reply output cannot claim an already-sent message"
            )
        allowed_ids = set(captured_ids)
        target_ids = _reply_target_message_ids(output.target_message_ids)
        if any(message_log_id not in allowed_ids for message_log_id in target_ids):
            raise ActorActiveReplyWorkflowError(
                "actor active reply output targets uncaptured input"
            )
        intents = tuple(output.external_action_intents)
        if output.replied and not intents:
            raise ActorActiveReplyWorkflowError(
                "actor active reply output claimed a visible response without an intent"
            )
        if intents and not output.replied:
            raise ActorActiveReplyWorkflowError(
                "actor active reply output proposed an action without a reply decision"
            )
        if not output.replied:
            if output.reason != "no_reply_tool":
                raise ActorActiveReplyWorkflowError(
                    "actor active reply no-reply outcome lacks a terminal no_reply tool"
                )
            if target_ids != captured_ids:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply no-reply outcome changed the captured target set"
                )
        seen_proposal_ids: set[str] = set()
        reply_count = 0
        for ordinal, intent in enumerate(intents):
            if not isinstance(intent, ExternalActionIntent):
                raise ActorActiveReplyWorkflowError(
                    "actor active reply output contains an invalid external action"
                )
            if intent.action_ordinal != ordinal:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply action ordinals must be contiguous from zero"
                )
            if intent.tool_call_id in seen_proposal_ids:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply output contains duplicate action proposals"
                )
            seen_proposal_ids.add(intent.tool_call_id)
            if intent.kind is ExternalActionKind.SEND_POKE:
                raise ActorActiveReplyWorkflowError(
                    "actor active reply v1 does not permit unbound send_poke intents"
                )
            if intent.kind is ExternalActionKind.SEND_REPLY:
                reply_count += 1
                if reply_count > 1:
                    raise ActorActiveReplyWorkflowError(
                        "actor active reply v1 permits at most one send_reply intent"
                    )
                _validate_reply_intent(
                    intent,
                    captured_ids=allowed_ids,
                )
                continue
            if intent.kind is ExternalActionKind.SEND_REACTION:
                _validate_reaction_intent(intent, captured_ids=allowed_ids)
                continue
            raise ActorActiveReplyWorkflowError(
                "actor active reply output contains an unsupported external action"
            )


def build_actor_active_reply_decision_runner(
    model_runtime: Any,
    *,
    prompt_registry: PromptRegistry,
    config: RunnerTemplateConfig | None = None,
    tool_manager: Any | None = None,
    message_formatter: MessageFormatterService | None = None,
) -> LLMReplyDecisionStageRunner:
    """Build the only LLM runner allowed by the Actor active-reply slice.

    Active reply and review share the same constrained reply-decision grammar.
    Reusing the central builder keeps their safety properties identical: no
    configured extension tools, no direct visible action execution, and no
    model repair attempt.
    """

    routing = replace(
        config or RunnerTemplateConfig(caller="agent.active_reply"),
        workflow_id="active_reply",
    )
    return build_actor_review_reply_decision_runner(
        model_runtime,
        prompt_registry=prompt_registry,
        config=routing,
        tool_manager=tool_manager,
        message_formatter=message_formatter,
    )


def _stage_message_log_ids(
    messages: Sequence[Mapping[str, object]],
) -> tuple[int, ...]:
    """Read exact ordered durable identities from one projected stage."""

    result: list[int] = []
    for message in messages:
        if not isinstance(message, Mapping):
            raise ActorActiveReplyWorkflowError(
                "actor active reply stage input contains an invalid message payload"
            )
        message_log_id = message.get("id")
        if (
            isinstance(message_log_id, bool)
            or not isinstance(message_log_id, int)
            or message_log_id < 1
        ):
            raise ActorActiveReplyWorkflowError(
                "actor active reply stage input contains an invalid message log id"
            )
        result.append(message_log_id)
    if len(set(result)) != len(result):
        raise ActorActiveReplyWorkflowError(
            "actor active reply stage input contains duplicate message log ids"
        )
    return tuple(result)


def _ledger_ordered_selected_message_log_ids(
    request: ActiveReplyWorkflowRequest,
) -> tuple[int, ...]:
    """Return the exact selected IDs in durable ledger order.

    The request ID list is a selection fence, not a model-visible ordering
    authority.  The adapter normally binds ``effect.ledger_entries`` to this
    exact selection, but the workflow repeats the equality check so a custom
    projector cannot make a request order influence the prompt or consumption
    record.
    """

    ledger_ids = request.effect.message_log_ids
    request_ids = request.message_log_ids
    if len(ledger_ids) != len(request_ids) or set(ledger_ids) != set(request_ids):
        raise ActorActiveReplyWorkflowError(
            "actor active reply request ids do not match its selected ledger entries"
        )
    return ledger_ids


def _metadata_message_log_ids(
    metadata: Mapping[str, object],
    key: str,
) -> tuple[int, ...]:
    """Decode one exact ordered ID list without coercing prompt metadata."""

    raw = metadata.get(key)
    if not isinstance(raw, list):
        raise ActorActiveReplyWorkflowError(
            f"actor active reply stage metadata omitted {key}"
        )
    ids: list[int] = []
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ActorActiveReplyWorkflowError(
                f"actor active reply stage metadata has an invalid {key} value"
            )
        ids.append(value)
    if len(set(ids)) != len(ids):
        raise ActorActiveReplyWorkflowError(
            f"actor active reply stage metadata has duplicate {key} values"
        )
    return tuple(ids)


def _reply_target_message_ids(values: Sequence[object]) -> tuple[int, ...]:
    """Validate model target diagnostics against the effect-captured input."""

    result: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ActorActiveReplyWorkflowError(
                "actor active reply output contains an invalid target message id"
            )
        result.append(value)
    if len(set(result)) != len(result):
        raise ActorActiveReplyWorkflowError(
            "actor active reply output contains duplicate target message ids"
        )
    return tuple(result)


def _reply_output_requires_failure(reason: object) -> bool:
    """Identify runner failures that must leave actor input unread."""

    if not isinstance(reason, str):
        return True
    normalized = reason.strip()
    if normalized in {
        "llm_reply_decision_failed",
        "llm_reply_decision_no_terminal_tool",
    }:
        return True
    return normalized.startswith(
        (
            "llm_reply_tool_call_skipped_",
            "reaction_tool_",
            "reply_external_action_invalid:",
            "reply_tool_",
        )
    )


def _validate_reply_intent(
    intent: ExternalActionIntent,
    *,
    captured_ids: set[int],
) -> None:
    """Require replies to bind their quote to one captured durable log ID."""

    payload = intent.payload
    if "quote_message_id" in payload:
        raise ActorActiveReplyWorkflowError(
            "actor active reply intent cannot use an unbound platform quote id"
        )
    quote_message_log_id = payload.get("quote_message_log_id")
    if quote_message_log_id is None:
        raise ActorActiveReplyWorkflowError(
            "actor active reply reply intent requires a captured quote message id"
        )
    if (
        isinstance(quote_message_log_id, bool)
        or not isinstance(quote_message_log_id, int)
        or quote_message_log_id not in captured_ids
    ):
        raise ActorActiveReplyWorkflowError(
            "actor active reply intent quotes a message outside captured input"
        )


def _validate_reaction_intent(
    intent: ExternalActionIntent,
    *,
    captured_ids: set[int],
) -> None:
    """Require reactions to target one captured durable log ID."""

    payload = intent.payload
    if "message_id" in payload:
        raise ActorActiveReplyWorkflowError(
            "actor active reply reaction intent cannot use an unbound platform message id"
        )
    message_log_id = payload.get("message_log_id")
    if (
        isinstance(message_log_id, bool)
        or not isinstance(message_log_id, int)
        or message_log_id not in captured_ids
    ):
        raise ActorActiveReplyWorkflowError(
            "actor active reply reaction intent targets a message outside captured input"
        )


__all__ = [
    "ActorActiveReplyWorkflowError",
    "ActorActiveReplyWorkflowStageInputProjector",
    "RunnerActiveReplyWorkflow",
    "build_actor_active_reply_decision_runner",
]
