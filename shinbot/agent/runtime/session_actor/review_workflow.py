"""Actor-native first review workflow with deferred external actions only.

This intentionally narrow first slice does not reuse the legacy review
coordinator. It makes one reply-decision model call over the effect-captured
ledger snapshot, returns only deferred action intents, and always settles back
to idle. Active-chat bootstrap, summaries, compression, and scan orchestration
remain separate Actor v2 verticals.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
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
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ReviewWorkflowOutput,
    ReviewWorkflowPort,
    ReviewWorkflowRequest,
    ReviewWorkflowWindowOutput,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ReviewNextReviewOutcome,
    ReviewNextReviewOutcomeKind,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.workflows.action_mode import ExternalActionToolMode

_REPLY_STAGE_PURPOSE = "reply_decision"


class ActorReviewWorkflowError(RuntimeError):
    """Raised when an Actor v2 review result cannot be safely completed."""


class ActorReviewWorkflowStageInputProjector(Protocol):
    """Build the exact review-stage input already fenced by an actor effect."""

    async def build_review_stage_input(
        self,
        request: ReviewWorkflowRequest,
    ) -> ReviewStageInput:
        """Return one immutable-boundary reply-decision stage input."""


@dataclass(slots=True, frozen=True)
class ActorReviewWorkflowConfig:
    """Static non-model controls for the first Actor v2 review slice."""

    completion_reason: str = "actor_review_completed"
    empty_input_reason: str = "actor_review_no_captured_messages"
    reply_window_id: str = "captured-ledger"

    def __post_init__(self) -> None:
        """Require stable bounded identifiers for durable completion diagnostics."""

        for field_name in (
            "completion_reason",
            "empty_input_reason",
            "reply_window_id",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())


class RunnerReviewWorkflow(ReviewWorkflowPort):
    """Bridge a fenced context projector to one collect-intents reply runner."""

    def __init__(
        self,
        *,
        projector: ActorReviewWorkflowStageInputProjector,
        reply_runner: ReplyDecisionStageRunner,
        config: ActorReviewWorkflowConfig | None = None,
    ) -> None:
        self._projector = projector
        self._reply_runner = reply_runner
        self._config = config or ActorReviewWorkflowConfig()

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Run exactly one safe reply-decision step for captured unread input."""

        if not isinstance(request, ReviewWorkflowRequest):
            raise TypeError("request must be ReviewWorkflowRequest")
        if request.external_action_mode is not ExternalActionToolMode.COLLECT_INTENTS:
            raise ActorReviewWorkflowError(
                "actor review workflow must collect external action intents"
            )
        captured_ids = request.effect.message_log_ids
        if not captured_ids:
            return self._completed_output(
                consumed_message_log_ids=(),
                reason=self._config.empty_input_reason,
            )

        stage_input = await self._projector.build_review_stage_input(request)
        self._validate_stage_input(stage_input, request=request)
        stage_output = await self._reply_runner.run(stage_input)
        if not isinstance(stage_output, ReplyDecisionStageOutput):
            raise TypeError("actor review reply runner returned an invalid output type")
        self._validate_reply_output(stage_output, captured_ids=captured_ids)
        windows: tuple[ReviewWorkflowWindowOutput, ...] = ()
        if stage_output.external_action_intents:
            windows = (
                ReviewWorkflowWindowOutput(
                    window_id=self._config.reply_window_id,
                    external_action_intents=stage_output.external_action_intents,
                ),
            )
        return self._completed_output(
            consumed_message_log_ids=captured_ids,
            reason=self._config.completion_reason,
            reply_windows=windows,
            model_execution_id=stage_output.model_execution_id,
            prompt_signature=stage_output.prompt_signature,
        )

    def _completed_output(
        self,
        *,
        consumed_message_log_ids: tuple[int, ...],
        reason: str,
        reply_windows: tuple[ReviewWorkflowWindowOutput, ...] = (),
        model_execution_id: str = "",
        prompt_signature: str = "",
    ) -> ReviewWorkflowOutput:
        """Return an idle-only completion without allowing model schedule control."""

        return ReviewWorkflowOutput(
            enter_active_chat=False,
            next_review_outcome=ReviewNextReviewOutcome(
                kind=ReviewNextReviewOutcomeKind.DEFAULTED,
                applied_delay_seconds=0.0,
                reason=reason,
                fallback_reason="actor_review_default_schedule",
            ),
            consumed_message_log_ids=consumed_message_log_ids,
            reply_windows=reply_windows,
            model_execution_id=model_execution_id,
            prompt_signature=prompt_signature,
        )

    @staticmethod
    def _validate_stage_input(
        stage_input: object,
        *,
        request: ReviewWorkflowRequest,
    ) -> None:
        """Prove a projector did not widen or retarget model-visible input."""

        if not isinstance(stage_input, ReviewStageInput):
            raise ActorReviewWorkflowError(
                "actor review projector returned an invalid stage input"
            )
        effect = request.effect
        if stage_input.session_id != effect.key.session_id:
            raise ActorReviewWorkflowError(
                "actor review projector changed the actor session id"
            )
        if stage_input.instance_id != effect.instance_id:
            raise ActorReviewWorkflowError(
                "actor review projector changed the adapter instance id"
            )
        if stage_input.purpose != _REPLY_STAGE_PURPOSE:
            raise ActorReviewWorkflowError(
                "actor review projector changed the stage purpose"
            )
        expected_ids = request.effect.message_log_ids
        source_ids = _stage_message_log_ids(stage_input.source_messages)
        if source_ids != expected_ids:
            raise ActorReviewWorkflowError(
                "actor review projector changed the captured message selection"
            )
        for metadata_key in ("ledger_message_log_ids", "candidate_message_ids"):
            if _metadata_message_log_ids(stage_input.metadata, metadata_key) != expected_ids:
                raise ActorReviewWorkflowError(
                    "actor review projector changed " + metadata_key
                )
        if stage_input.metadata.get("actor_v2") is not True:
            raise ActorReviewWorkflowError(
                "actor review projector omitted actor_v2 provenance"
            )
        if stage_input.metadata.get("operation_id") != effect.operation_id:
            raise ActorReviewWorkflowError(
                "actor review projector changed the operation id"
            )
        if stage_input.metadata.get("ownership_generation") != effect.ownership_generation:
            raise ActorReviewWorkflowError(
                "actor review projector changed the ownership generation"
            )
        if stage_input.metadata.get("input_watermark") != effect.input_watermark:
            raise ActorReviewWorkflowError(
                "actor review projector changed the watermark fence"
            )
        if (
            stage_input.metadata.get("input_ledger_sequence")
            != effect.input_ledger_sequence
        ):
            raise ActorReviewWorkflowError(
                "actor review projector changed the sequence fence"
            )

    @staticmethod
    def _validate_reply_output(
        output: ReplyDecisionStageOutput,
        *,
        captured_ids: tuple[int, ...],
    ) -> None:
        """Reject reply decisions that cannot become receipt-fenced actions."""

        if output.consumption_deferred:
            raise ActorReviewWorkflowError(
                "actor review reply output cannot defer ledger consumption"
            )
        if _reply_output_requires_failure(output.reason):
            raise ActorReviewWorkflowError(
                "actor review reply decision failed: " + str(output.reason)
            )
        if output.reply_message_id is not None or output.reply_message_ids:
            raise ActorReviewWorkflowError(
                "actor review reply output cannot claim an already-sent message"
            )
        allowed_ids = set(captured_ids)
        target_ids = _reply_target_message_ids(output.target_message_ids)
        if any(message_log_id not in allowed_ids for message_log_id in target_ids):
            raise ActorReviewWorkflowError(
                "actor review reply output targets uncaptured input"
            )
        intents = tuple(output.external_action_intents)
        if output.replied and not intents:
            raise ActorReviewWorkflowError(
                "actor review reply output claimed a visible response without an intent"
            )
        seen_proposal_ids: set[str] = set()
        reply_count = 0
        for ordinal, intent in enumerate(intents):
            if not isinstance(intent, ExternalActionIntent):
                raise ActorReviewWorkflowError(
                    "actor review reply output contains an invalid external action"
                )
            if intent.action_ordinal != ordinal:
                raise ActorReviewWorkflowError(
                    "actor review reply action ordinals must be contiguous from zero"
                )
            if intent.tool_call_id in seen_proposal_ids:
                raise ActorReviewWorkflowError(
                    "actor review reply output contains duplicate action proposals"
                )
            seen_proposal_ids.add(intent.tool_call_id)
            if intent.kind is ExternalActionKind.SEND_POKE:
                raise ActorReviewWorkflowError(
                    "actor review v1 does not permit unbound send_poke intents"
                )
            if intent.kind is ExternalActionKind.SEND_REPLY:
                reply_count += 1
                _validate_reply_intent(
                    intent,
                    captured_ids=allowed_ids,
                    require_quote=(reply_count == 1),
                )
                continue
            if intent.kind is ExternalActionKind.SEND_REACTION:
                _validate_reaction_intent(intent, captured_ids=allowed_ids)
                continue
            raise ActorReviewWorkflowError(
                "actor review reply output contains an unsupported external action"
            )


def build_actor_review_reply_decision_runner(
    model_runtime: Any,
    *,
    prompt_registry: PromptRegistry,
    config: RunnerTemplateConfig | None = None,
    tool_manager: Any | None = None,
    message_formatter: MessageFormatterService | None = None,
) -> LLMReplyDecisionStageRunner:
    """Build the only LLM reply runner allowed by the first Actor review slice.

    The model sees only the hard-coded reply action schemas. It cannot receive
    configured extension tools, execute a tool, or issue a repair model call.
    """

    routing = replace(config or RunnerTemplateConfig(), max_model_retries=0)
    return LLMReplyDecisionStageRunner(
        model_runtime,
        config=routing,
        prompt_registry=prompt_registry,
        tool_manager=tool_manager,
        message_formatter=message_formatter,
        external_action_mode=ExternalActionToolMode.COLLECT_INTENTS,
        allow_configured_extra_tools=False,
        max_repair_attempts=0,
    )


def _stage_message_log_ids(
    messages: Sequence[Mapping[str, object]],
) -> tuple[int, ...]:
    """Read exact ordered durable message identities from a projected stage."""

    result: list[int] = []
    for message in messages:
        if not isinstance(message, Mapping):
            raise ActorReviewWorkflowError(
                "actor review stage input contains an invalid message payload"
            )
        message_log_id = message.get("id")
        if (
            isinstance(message_log_id, bool)
            or not isinstance(message_log_id, int)
            or message_log_id < 1
        ):
            raise ActorReviewWorkflowError(
                "actor review stage input contains an invalid message log id"
            )
        result.append(message_log_id)
    if len(set(result)) != len(result):
        raise ActorReviewWorkflowError(
            "actor review stage input contains duplicate message log ids"
        )
    return tuple(result)


def _metadata_message_log_ids(
    metadata: Mapping[str, object],
    key: str,
) -> tuple[int, ...]:
    """Decode one exact ordered ID list without coercing model-visible data."""

    raw = metadata.get(key)
    if not isinstance(raw, list):
        raise ActorReviewWorkflowError(f"actor review stage metadata omitted {key}")
    ids: list[int] = []
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ActorReviewWorkflowError(
                f"actor review stage metadata has an invalid {key} value"
            )
        ids.append(value)
    if len(set(ids)) != len(ids):
        raise ActorReviewWorkflowError(
            f"actor review stage metadata has duplicate {key} values"
        )
    return tuple(ids)


def _reply_target_message_ids(values: Sequence[object]) -> tuple[int, ...]:
    """Validate optional model target diagnostics against captured input."""

    result: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ActorReviewWorkflowError(
                "actor review reply output contains an invalid target message id"
            )
        result.append(value)
    if len(set(result)) != len(result):
        raise ActorReviewWorkflowError(
            "actor review reply output contains duplicate target message ids"
        )
    return tuple(result)


def _reply_output_requires_failure(reason: object) -> bool:
    """Identify runner failures that must not consume Actor-captured input."""

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
    require_quote: bool,
) -> None:
    """Ensure reply quotes have a durable, captured message identity."""

    payload = intent.payload
    if "quote_message_id" in payload:
        raise ActorReviewWorkflowError(
            "actor review reply intent cannot use an unbound platform quote id"
        )
    quote_message_log_id = payload.get("quote_message_log_id")
    if quote_message_log_id is None:
        if require_quote:
            raise ActorReviewWorkflowError(
                "actor review first reply intent requires a captured quote message id"
            )
        return
    if (
        isinstance(quote_message_log_id, bool)
        or not isinstance(quote_message_log_id, int)
        or quote_message_log_id not in captured_ids
    ):
        raise ActorReviewWorkflowError(
            "actor review reply intent quotes a message outside captured input"
        )


def _validate_reaction_intent(
    intent: ExternalActionIntent,
    *,
    captured_ids: set[int],
) -> None:
    """Require reactions to name a captured durable message rather than raw IDs."""

    payload = intent.payload
    if "message_id" in payload:
        raise ActorReviewWorkflowError(
            "actor review reaction intent cannot use an unbound platform message id"
        )
    message_log_id = payload.get("message_log_id")
    if (
        isinstance(message_log_id, bool)
        or not isinstance(message_log_id, int)
        or message_log_id not in captured_ids
    ):
        raise ActorReviewWorkflowError(
            "actor review reaction intent targets a message outside captured input"
        )


__all__ = [
    "ActorReviewWorkflowConfig",
    "ActorReviewWorkflowError",
    "ActorReviewWorkflowStageInputProjector",
    "RunnerReviewWorkflow",
    "build_actor_review_reply_decision_runner",
]
