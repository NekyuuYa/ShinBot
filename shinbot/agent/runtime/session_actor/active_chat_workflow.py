"""Actor-native v3 Active Chat workflows with deferred effects only.

The legacy active-chat path combines an in-memory coordinator, a mutable
message buffer, model calls, tool execution, and platform delivery in one
background task.  Actor v3 deliberately does none of that.  Each workflow in
this module receives an immutable actor effect request, projects exactly its
frozen message selection, and returns a typed completion candidate.  Visible
actions are represented solely by :class:`ExternalActionIntent`; the actor
accepts them before the outbox/receipt path can dispatch anything externally.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from shinbot.agent.runners.review_bootstrap.prompt_registration import (
    REVIEW_BOOTSTRAP_COMPONENT_IDS,
)
from shinbot.agent.runners.templates import (
    RunnerTemplateConfig,
    StructuredOutputRunner,
    ToolCallPlanResult,
    ToolCallPlanRunner,
    parse_tool_call_payload,
)
from shinbot.agent.runners.templates.structured_output import StructuredOutputRun
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveChatBootstrapWorkflowOutput,
    ActiveChatBootstrapWorkflowPort,
    ActiveChatBootstrapWorkflowRequest,
    ActiveChatRoundWorkflowOutput,
    ActiveChatRoundWorkflowPort,
    ActiveChatRoundWorkflowRequest,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapDisposition,
    ActiveChatRoundOutcome,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import json_schema_response_format
from shinbot.agent.workflows.action_mode import ExternalActionToolMode
from shinbot.agent.workflows.active_chat.prompt_registration import (
    ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS,
)
from shinbot.agent.workflows.chat_actions import (
    CHAT_ACTION_TOOL_TAG,
    collect_external_action_intent,
)

_BOOTSTRAP_STAGE_PURPOSE = "active_chat_bootstrap"
_ROUND_STAGE_PURPOSE = "active_chat_round"
_ACTOR_NATIVE_MARKER = "active_chat_v3"

_BOOTSTRAP_RESPONSE_FORMAT = json_schema_response_format(
    "actor_active_chat_bootstrap",
    {
        "disposition": {
            "type": "string",
            "enum": [item.value for item in ActiveChatBootstrapDisposition],
        },
        "reason": {"type": "string"},
    },
    ["disposition", "reason"],
)


class ActorActiveChatWorkflowError(RuntimeError):
    """Raised when an Actor v3 Active Chat result cannot be fenced safely."""


class ActorActiveChatBootstrapStageInputProjector(Protocol):
    """Project one immutable review handoff into a bootstrap stage input."""

    async def build_active_chat_bootstrap_stage_input(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ReviewStageInput:
        """Return a stage input containing exactly the frozen handoff messages."""


class ActorActiveChatRoundStageInputProjector(Protocol):
    """Project one immutable selected unread range into a round stage input."""

    async def build_active_chat_round_stage_input(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ReviewStageInput:
        """Return a stage input containing exactly the selected unread messages."""


class ActorActiveChatRoundPlanRunner(ToolCallPlanRunner):
    """Tool planner whose only non-action control is local active-chat exit.

    ``ToolCallPlanRunner`` invokes the model but never invokes a tool.  The
    inherited implementation is used with no repair and no configured extras;
    the extra virtual schema is intentionally a local control decision rather
    than an executable tool.
    """

    external_action_mode = ExternalActionToolMode.COLLECT_INTENTS

    def _build_tools(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
        """Return the fixed visible-action grammar plus local exit control."""

        tools = super()._build_tools(stage_input)
        if any(_tool_name(tool) == "exit_active" for tool in tools):
            return tools
        return [*tools, _exit_active_tool_schema()]


class RunnerActiveChatBootstrapWorkflow(ActiveChatBootstrapWorkflowPort):
    """Run one structured bootstrap decision over an exact review handoff."""

    def __init__(
        self,
        *,
        projector: ActorActiveChatBootstrapStageInputProjector,
        bootstrap_runner: StructuredOutputRunner,
    ) -> None:
        self._projector = projector
        self._bootstrap_runner = bootstrap_runner

    async def run_active_chat_bootstrap(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ActiveChatBootstrapWorkflowOutput:
        """Return one discrete disposition and model-call provenance.

        The bootstrap effect has no mutable active-chat state to read and no
        visible action capability.  An invalid model response is an effect
        failure, allowing the reducer's existing fail-closed bootstrap path to
        request a normal active-chat exit.
        """

        if not isinstance(request, ActiveChatBootstrapWorkflowRequest):
            raise TypeError("request must be ActiveChatBootstrapWorkflowRequest")
        stage_input = await self._projector.build_active_chat_bootstrap_stage_input(
            request
        )
        self._validate_stage_input(stage_input, request=request)

        run = await self._bootstrap_runner.run_with_provenance(stage_input)
        if not isinstance(run, StructuredOutputRun):
            raise TypeError("active chat bootstrap runner returned an invalid result")
        payload = run.payload
        if not isinstance(payload, Mapping):
            raise ActorActiveChatWorkflowError(
                "active chat bootstrap model did not return structured output"
            )
        unknown = sorted(set(payload).difference({"disposition", "reason"}))
        if unknown:
            raise ActorActiveChatWorkflowError(
                "active chat bootstrap output contains unsupported fields: "
                + ", ".join(unknown)
            )
        try:
            disposition = ActiveChatBootstrapDisposition(payload.get("disposition"))
        except (ActorActiveChatWorkflowError, TypeError, ValueError) as exc:
            raise ActorActiveChatWorkflowError(
                "active chat bootstrap output has an invalid disposition"
            ) from exc
        reason = _required_text(payload.get("reason"), field_name="bootstrap.reason")
        return ActiveChatBootstrapWorkflowOutput(
            disposition=disposition,
            reason=reason,
            model_execution_id=run.model_execution_id,
            prompt_signature=run.prompt_signature,
        )

    @staticmethod
    def _validate_stage_input(
        stage_input: object,
        *,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> None:
        """Prove that bootstrap context did not widen the review handoff."""

        _validate_actor_stage_input(
            stage_input,
            effect=request.effect,
            purpose=_BOOTSTRAP_STAGE_PURPOSE,
            expected_message_log_ids=request.handoff_message_log_ids,
            expected_metadata={
                "active_epoch": request.active_epoch,
                "handoff_operation_id": request.handoff_operation_id,
            },
            selection_metadata_keys=(
                "ledger_message_log_ids",
                "candidate_message_ids",
                "handoff_message_log_ids",
            ),
            selection_order_must_match=False,
            workflow_name="active chat bootstrap",
        )


class RunnerActiveChatRoundWorkflow(ActiveChatRoundWorkflowPort):
    """Run one safe Active Chat round over an exact unread ledger selection."""

    def __init__(
        self,
        *,
        projector: ActorActiveChatRoundStageInputProjector,
        plan_runner: ToolCallPlanRunner,
    ) -> None:
        self._projector = projector
        self._plan_runner = plan_runner

    async def run_active_chat_round(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ActiveChatRoundWorkflowOutput:
        """Plan exactly one local outcome and collect, never execute, an intent."""

        if not isinstance(request, ActiveChatRoundWorkflowRequest):
            raise TypeError("request must be ActiveChatRoundWorkflowRequest")
        if request.external_action_mode is not ExternalActionToolMode.COLLECT_INTENTS:
            raise ActorActiveChatWorkflowError(
                "actor active chat round must collect external action intents"
            )
        selected_message_log_ids = _ledger_ordered_selected_message_log_ids(request)
        stage_input = await self._projector.build_active_chat_round_stage_input(request)
        self._validate_stage_input(
            stage_input,
            request=request,
            selected_message_log_ids=selected_message_log_ids,
        )
        plan = await self._plan_runner.run(stage_input)
        if not isinstance(plan, ToolCallPlanResult):
            raise TypeError("active chat round planner returned an invalid result")

        output = self._output_from_plan(
            plan,
            selected_message_log_ids=selected_message_log_ids,
        )
        _validate_round_output(
            output,
            selected_message_log_ids=selected_message_log_ids,
        )
        return output

    @staticmethod
    def _validate_stage_input(
        stage_input: object,
        *,
        request: ActiveChatRoundWorkflowRequest,
        selected_message_log_ids: tuple[int, ...],
    ) -> None:
        """Prove that a round projector did not widen or retarget input."""

        _validate_actor_stage_input(
            stage_input,
            effect=request.effect,
            purpose=_ROUND_STAGE_PURPOSE,
            expected_message_log_ids=selected_message_log_ids,
            expected_metadata={
                "active_epoch": request.active_epoch,
                "round_schedule_id": request.round_schedule_id,
                "interest_value": request.interest_value,
                "active_chat_interest_value": request.interest_value,
                "bootstrap_disposition": request.bootstrap_disposition,
                "message_log_ids": list(selected_message_log_ids),
            },
            workflow_name="active chat round",
        )

    def _output_from_plan(
        self,
        plan: ToolCallPlanResult,
        *,
        selected_message_log_ids: tuple[int, ...],
    ) -> ActiveChatRoundWorkflowOutput:
        """Map one constrained model plan into a fenced completion candidate."""

        provenance = _plan_provenance(plan)
        if not plan.has_tool_calls:
            return _retry_output(
                _retry_reason(plan.reason, fallback="active_chat_round_no_terminal_tool"),
                **provenance,
            )
        if any(
            not _has_strict_tool_call_arguments(tool_call)
            for tool_call in plan.tool_calls
        ):
            return _retry_output(
                "active_chat_round_malformed_tool_arguments",
                **provenance,
            )
        try:
            parsed_calls = [parse_tool_call_payload(call) for call in plan.tool_calls]
        except (TypeError, ValueError) as exc:
            return _retry_output(
                "active_chat_round_invalid_tool_payload:" + type(exc).__name__,
                **provenance,
            )
        if len(parsed_calls) != 1:
            return _retry_output(
                "active_chat_round_requires_exactly_one_terminal_tool",
                **provenance,
            )
        call = parsed_calls[0]
        if not isinstance(call.arguments, Mapping):
            return _retry_output(
                "active_chat_round_tool_arguments_must_be_an_object",
                **provenance,
            )
        name = call.name.strip()
        if name == "send_poke":
            return _retry_output("active_chat_round_poke_forbidden", **provenance)
        if name == "no_reply":
            return self._no_reply_output(
                arguments=call.arguments,
                selected_message_log_ids=selected_message_log_ids,
                **provenance,
            )
        if name == "exit_active":
            return self._exit_output(
                arguments=call.arguments,
                selected_message_log_ids=selected_message_log_ids,
                **provenance,
            )
        if name not in {"send_reply", "send_reaction"}:
            return _retry_output(
                "active_chat_round_unsupported_tool:" + (name or "missing"),
                **provenance,
            )
        try:
            intent = collect_external_action_intent(
                tool_call_id=_tool_call_id(call.raw),
                tool_name=name,
                arguments=call.arguments,
                action_ordinal=0,
            )
        except (ActorActiveChatWorkflowError, TypeError, ValueError) as exc:
            return _retry_output(
                "active_chat_round_invalid_" + name + ":" + type(exc).__name__,
                **provenance,
            )

        delta = 2.0 if intent.kind is ExternalActionKind.SEND_REACTION else 5.0
        if intent.kind is ExternalActionKind.SEND_REPLY:
            intensity = _optional_text(call.arguments.get("intensity"))
            if intensity and intensity not in {"light", "engaged"}:
                return _retry_output(
                    "active_chat_round_invalid_reply_intensity", **provenance
                )
            if intensity == "engaged":
                delta = 10.0
        output = ActiveChatRoundWorkflowOutput(
            outcome=ActiveChatRoundOutcome.CONTINUE,
            interest_delta=delta,
            reason=_action_reason(call.arguments, fallback=name),
            consumed_message_log_ids=selected_message_log_ids,
            external_action_intents=(intent,),
            **provenance,
        )
        try:
            _validate_round_output(
                output,
                selected_message_log_ids=selected_message_log_ids,
            )
        except ActorActiveChatWorkflowError as exc:
            return _retry_output(
                "active_chat_round_invalid_" + name + ":" + str(exc),
                **provenance,
            )
        return output

    @staticmethod
    def _no_reply_output(
        *,
        arguments: Mapping[str, Any],
        selected_message_log_ids: tuple[int, ...],
        model_execution_id: str,
        prompt_signature: str,
    ) -> ActiveChatRoundWorkflowOutput:
        """Map a no-reply semantic decision to its bounded interest delta."""

        allowed = {"intensity", "reason", "terminate_round"}
        if unknown := sorted(set(arguments).difference(allowed)):
            return _retry_output(
                "active_chat_round_invalid_no_reply_fields:" + ",".join(unknown),
                model_execution_id=model_execution_id,
                prompt_signature=prompt_signature,
            )
        intensity = _optional_text(arguments.get("intensity"))
        if intensity not in {"", "normal", "strong"}:
            return _retry_output(
                "active_chat_round_invalid_no_reply_intensity",
                model_execution_id=model_execution_id,
                prompt_signature=prompt_signature,
            )
        return ActiveChatRoundWorkflowOutput(
            outcome=ActiveChatRoundOutcome.CONTINUE,
            interest_delta=-10.0 if intensity == "strong" else -5.0,
            reason=_action_reason(arguments, fallback="no_reply"),
            consumed_message_log_ids=selected_message_log_ids,
            model_execution_id=model_execution_id,
            prompt_signature=prompt_signature,
        )

    @staticmethod
    def _exit_output(
        *,
        arguments: Mapping[str, Any],
        selected_message_log_ids: tuple[int, ...],
        model_execution_id: str,
        prompt_signature: str,
    ) -> ActiveChatRoundWorkflowOutput:
        """Map the local virtual exit control to a no-action exit result."""

        if sorted(set(arguments).difference({"reason"})):
            return _retry_output(
                "active_chat_round_invalid_exit_fields",
                model_execution_id=model_execution_id,
                prompt_signature=prompt_signature,
            )
        try:
            reason = _required_text(arguments.get("reason"), field_name="exit.reason")
        except ActorActiveChatWorkflowError:
            return _retry_output(
                "active_chat_round_exit_requires_reason",
                model_execution_id=model_execution_id,
                prompt_signature=prompt_signature,
            )
        return ActiveChatRoundWorkflowOutput(
            outcome=ActiveChatRoundOutcome.EXIT,
            interest_delta=0.0,
            reason=reason,
            consumed_message_log_ids=selected_message_log_ids,
            model_execution_id=model_execution_id,
            prompt_signature=prompt_signature,
        )


def build_actor_active_chat_bootstrap_runner(
    model_runtime: Any,
    *,
    prompt_registry: PromptRegistry,
    config: RunnerTemplateConfig | None = None,
    message_formatter: MessageFormatterService | None = None,
) -> StructuredOutputRunner:
    """Build the single-call structured runner for Actor-native bootstrap."""

    routing = config or RunnerTemplateConfig(
        caller="agent.active_chat.bootstrap",
        workflow_id="active_chat_bootstrap",
    )
    return StructuredOutputRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        config=_actor_template_config(
            routing,
            workflow_id="active_chat_bootstrap",
            response_format=_BOOTSTRAP_RESPONSE_FORMAT,
            builtin_component_ids=REVIEW_BOOTSTRAP_COMPONENT_IDS,
        ),
        message_formatter=message_formatter,
    )


def build_actor_active_chat_round_plan_runner(
    model_runtime: Any,
    *,
    prompt_registry: PromptRegistry,
    tool_manager: Any,
    config: RunnerTemplateConfig | None = None,
    message_formatter: MessageFormatterService | None = None,
) -> ActorActiveChatRoundPlanRunner:
    """Build a one-call, collect-intents-only Active Chat round planner.

    The returned planner never receives configured extension tools, has no
    repair pass, and has no execution dependency.  It may only plan one of
    ``no_reply``, ``send_reply``, ``send_reaction``, or local ``exit_active``.
    """

    routing = config or RunnerTemplateConfig(
        caller="agent.active_chat.round",
        workflow_id="active_chat_round",
    )
    return ActorActiveChatRoundPlanRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        config=_actor_template_config(
            routing,
            workflow_id="active_chat_round",
            builtin_component_ids=ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS["round"],
            inherit_component_ids=False,
        ),
        tool_manager=tool_manager,
        tool_names=["no_reply", "send_reply", "send_reaction"],
        repair_prompt="",
        max_repair_attempts=0,
        tool_transform=_actor_active_chat_round_tool_schema,
        tool_tags={CHAT_ACTION_TOOL_TAG},
        allow_configured_extra_tools=False,
        message_formatter=message_formatter,
    )


def _actor_template_config(
    routing: Any,
    *,
    workflow_id: str,
    response_format: dict[str, Any] | None = None,
    builtin_component_ids: dict[Any, list[str]] | None = None,
    inherit_component_ids: bool = True,
) -> RunnerTemplateConfig:
    """Copy compatible runner configuration while forcing one model attempt."""

    return RunnerTemplateConfig(
        caller=str(getattr(routing, "caller", "agent.active_chat")),
        workflow_id=workflow_id,
        llm=str(getattr(routing, "llm", "")),
        default_llm=str(getattr(routing, "default_llm", "")),
        route_id=getattr(routing, "route_id", None),
        model_id=getattr(routing, "model_id", None),
        profile_id=str(getattr(routing, "profile_id", "")),
        response_format=response_format,
        component_ids_by_stage=(
            dict(getattr(routing, "component_ids_by_stage", {}) or {})
            if inherit_component_ids
            else {}
        ),
        builtin_component_ids={
            stage: list(component_ids)
            for stage, component_ids in (builtin_component_ids or {}).items()
        },
        special_prompt_ids=dict(getattr(routing, "special_prompt_ids", {}) or {}),
        message_format_config=getattr(routing, "message_format_config", None),
        params=dict(getattr(routing, "params", {}) or {}),
        tool_config=routing.tool_config,
        max_model_retries=0,
        retry_backoff_seconds=getattr(routing, "retry_backoff_seconds", 0.25),
        instance_config_resolver=getattr(routing, "instance_config_resolver", None),
        model_target_resolver=getattr(routing, "model_target_resolver", None),
    )


def _validate_actor_stage_input(
    stage_input: object,
    *,
    effect: object,
    purpose: str,
    expected_message_log_ids: tuple[int, ...],
    expected_metadata: Mapping[str, object],
    selection_metadata_keys: tuple[str, ...] = (
        "ledger_message_log_ids",
        "candidate_message_ids",
    ),
    selection_order_must_match: bool = True,
    workflow_name: str,
) -> None:
    """Verify that all model-visible records remain inside actor fences."""

    if not isinstance(stage_input, ReviewStageInput):
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector returned an invalid stage input"
        )
    # ``effect`` is deliberately duck-typed only inside this shared validator;
    # request DTO construction already proves the concrete actor input type.
    key = getattr(effect, "key", None)
    if stage_input.session_id != getattr(key, "session_id", None):
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector changed the actor session id"
        )
    if stage_input.instance_id != getattr(effect, "instance_id", None):
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector changed the adapter instance id"
        )
    if stage_input.purpose != purpose:
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector changed the stage purpose"
        )
    if stage_input.context_messages or stage_input.instruction_content:
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector added unfenced prompt content"
        )
    stage_message_log_ids = _stage_message_log_ids(
        stage_input.source_messages,
        workflow_name=workflow_name,
    )
    selection_matches = (
        stage_message_log_ids == expected_message_log_ids
        if selection_order_must_match
        else (
            len(stage_message_log_ids) == len(expected_message_log_ids)
            and set(stage_message_log_ids) == set(expected_message_log_ids)
        )
    )
    if not selection_matches:
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector changed the captured message selection"
        )
    metadata = stage_input.metadata
    if not isinstance(metadata, Mapping):
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector returned invalid stage metadata"
        )
    expected_effect_metadata = {
        "operation_id": getattr(effect, "operation_id", None),
        "effect_id": getattr(effect, "effect_id", None),
        "ownership_generation": getattr(effect, "ownership_generation", None),
        "input_watermark": getattr(effect, "input_watermark", None),
        "input_ledger_sequence": getattr(effect, "input_ledger_sequence", None),
        "target_session_id": getattr(effect, "target_session_id", None),
    }
    allowed_metadata_keys = {
        "purpose",
        "actor_v2",
        _ACTOR_NATIVE_MARKER,
        *selection_metadata_keys,
        *expected_effect_metadata,
        *expected_metadata,
    }
    unknown_metadata = set(metadata).difference(allowed_metadata_keys)
    if unknown_metadata:
        raise ActorActiveChatWorkflowError(
            workflow_name
            + " projector added unfenced metadata: "
            + ", ".join(str(value) for value in sorted(unknown_metadata, key=str))
        )
    if "purpose" in metadata and metadata.get("purpose") != purpose:
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector changed metadata purpose"
        )
    for key_name in selection_metadata_keys:
        if _metadata_message_log_ids(
            metadata,
            key=key_name,
            workflow_name=workflow_name,
        ) != stage_message_log_ids:
            raise ActorActiveChatWorkflowError(
                workflow_name + " projector changed " + key_name
            )
    if metadata.get("actor_v2") is not True or metadata.get(_ACTOR_NATIVE_MARKER) is not True:
        raise ActorActiveChatWorkflowError(
            workflow_name + " projector omitted Actor-native provenance"
        )
    for key_name, expected in {**expected_effect_metadata, **expected_metadata}.items():
        if metadata.get(key_name) != expected:
            raise ActorActiveChatWorkflowError(
                workflow_name + " projector changed " + key_name
            )


def _ledger_ordered_selected_message_log_ids(
    request: ActiveChatRoundWorkflowRequest,
) -> tuple[int, ...]:
    """Return the exact request selection in durable ledger order."""

    ledger_ids = request.effect.message_log_ids
    requested_ids = request.message_log_ids
    if len(ledger_ids) != len(requested_ids) or set(ledger_ids) != set(requested_ids):
        raise ActorActiveChatWorkflowError(
            "active chat round request ids do not match its selected ledger entries"
        )
    return ledger_ids


def _validate_round_output(
    output: ActiveChatRoundWorkflowOutput,
    *,
    selected_message_log_ids: tuple[int, ...],
) -> None:
    """Enforce the actor v3 selection, action, and semantic delta contract."""

    selected = set(selected_message_log_ids)
    if output.outcome is ActiveChatRoundOutcome.RETRY:
        if output.consumed_message_log_ids or output.external_action_intents:
            raise ActorActiveChatWorkflowError(
                "retry active chat round cannot consume messages or propose actions"
            )
        if output.interest_delta != 0.0:
            raise ActorActiveChatWorkflowError(
                "retry active chat round must not adjust interest"
            )
        return
    if output.consumed_message_log_ids != selected_message_log_ids:
        raise ActorActiveChatWorkflowError(
            "active chat round must consume exactly its selected messages in ledger order"
        )
    intents = output.external_action_intents
    if output.outcome is ActiveChatRoundOutcome.EXIT:
        if intents or output.interest_delta != 0.0:
            raise ActorActiveChatWorkflowError(
                "active chat exit round cannot propose an action or adjust interest"
            )
        return
    if output.outcome is not ActiveChatRoundOutcome.CONTINUE:
        raise ActorActiveChatWorkflowError("active chat round has an unsupported outcome")
    if len(intents) > 1:
        raise ActorActiveChatWorkflowError(
            "actor active chat v3 permits at most one visible action"
        )
    if not intents:
        if output.interest_delta not in {-10.0, -5.0}:
            raise ActorActiveChatWorkflowError(
                "active chat no-reply round has an invalid interest adjustment"
            )
        return
    intent = intents[0]
    if not isinstance(intent, ExternalActionIntent) or intent.action_ordinal != 0:
        raise ActorActiveChatWorkflowError("active chat round action ordinal is invalid")
    payload = intent.payload
    if intent.kind is ExternalActionKind.SEND_REPLY:
        _validate_payload_fields(
            payload,
            allowed={"text", "quote_message_log_id"},
            action_name="send_reply",
        )
        if not isinstance(payload.get("text"), str) or not payload["text"].strip():
            raise ActorActiveChatWorkflowError(
                "active chat send_reply intent has invalid text"
            )
        _validate_selected_message_log_id(
            payload.get("quote_message_log_id"),
            selected=selected,
            field_name="quote_message_log_id",
        )
        if output.interest_delta not in {5.0, 10.0}:
            raise ActorActiveChatWorkflowError(
                "active chat reply has an invalid interest adjustment"
            )
        return
    if intent.kind is ExternalActionKind.SEND_REACTION:
        _validate_payload_fields(
            payload,
            allowed={"action", "emoji_id", "message_log_id"},
            action_name="send_reaction",
        )
        if payload.get("action") not in {"add", "remove"}:
            raise ActorActiveChatWorkflowError(
                "active chat send_reaction intent has invalid action"
            )
        if not isinstance(payload.get("emoji_id"), str) or not payload["emoji_id"].strip():
            raise ActorActiveChatWorkflowError(
                "active chat send_reaction intent has invalid emoji_id"
            )
        _validate_selected_message_log_id(
            payload.get("message_log_id"),
            selected=selected,
            field_name="message_log_id",
        )
        if output.interest_delta != 2.0:
            raise ActorActiveChatWorkflowError(
                "active chat reaction has an invalid interest adjustment"
            )
        return
    raise ActorActiveChatWorkflowError(
        "actor active chat v3 does not permit this external action"
    )


def _validate_payload_fields(
    payload: Mapping[str, object],
    *,
    allowed: set[str],
    action_name: str,
) -> None:
    """Reject raw platform identifiers and all unsupported action fields."""

    unknown = sorted(set(payload).difference(allowed))
    if unknown:
        raise ActorActiveChatWorkflowError(
            "active chat "
            + action_name
            + " intent has unsupported fields: "
            + ", ".join(unknown)
        )


def _validate_selected_message_log_id(
    value: object,
    *,
    selected: set[int],
    field_name: str,
) -> None:
    """Require a durable target from the exact frozen round selection."""

    if isinstance(value, bool) or not isinstance(value, int) or value not in selected:
        raise ActorActiveChatWorkflowError(
            "active chat action " + field_name + " is outside selected input"
        )


def _stage_message_log_ids(
    messages: Sequence[Mapping[str, object]],
    *,
    workflow_name: str,
) -> tuple[int, ...]:
    """Read exact ordered durable identities from an actor stage input."""

    result: list[int] = []
    for message in messages:
        if not isinstance(message, Mapping):
            raise ActorActiveChatWorkflowError(
                workflow_name + " stage input contains an invalid message payload"
            )
        message_log_id = message.get("id")
        if (
            isinstance(message_log_id, bool)
            or not isinstance(message_log_id, int)
            or message_log_id < 1
        ):
            raise ActorActiveChatWorkflowError(
                workflow_name + " stage input contains an invalid message log id"
            )
        result.append(message_log_id)
    if len(set(result)) != len(result):
        raise ActorActiveChatWorkflowError(
            workflow_name + " stage input contains duplicate message log ids"
        )
    return tuple(result)


def _metadata_message_log_ids(
    metadata: Mapping[str, object],
    *,
    key: str,
    workflow_name: str,
) -> tuple[int, ...]:
    """Decode one exact ordered metadata ID list without coercion."""

    raw = metadata.get(key)
    if not isinstance(raw, list):
        raise ActorActiveChatWorkflowError(
            workflow_name + " stage metadata omitted " + key
        )
    result: list[int] = []
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ActorActiveChatWorkflowError(
                workflow_name + " stage metadata has an invalid " + key + " value"
            )
        result.append(value)
    if len(set(result)) != len(result):
        raise ActorActiveChatWorkflowError(
            workflow_name + " stage metadata has duplicate " + key + " values"
        )
    return tuple(result)


def _plan_provenance(plan: ToolCallPlanResult) -> dict[str, str]:
    """Detach model execution and prompt identifiers from a planner result."""

    prompt_signature = ""
    if isinstance(plan.metadata, Mapping):
        prompt_signature = _optional_text(plan.metadata.get("prompt_signature"))
    return {
        "model_execution_id": _optional_text(plan.execution_id),
        "prompt_signature": prompt_signature,
    }


def _has_strict_tool_call_arguments(tool_call: object) -> bool:
    """Require the raw model arguments to decode to one JSON object.

    The generic planner parser intentionally maps malformed arguments to an
    empty dictionary for legacy compatibility.  Actor v3 must preserve the
    distinction because an empty ``no_reply`` payload is otherwise a valid
    consuming decision.
    """

    if not isinstance(tool_call, Mapping):
        return False
    function = tool_call.get("function")
    if not isinstance(function, Mapping):
        return False
    raw_arguments = function.get("arguments")
    if isinstance(raw_arguments, Mapping):
        return True
    if not isinstance(raw_arguments, str):
        return False
    try:
        decoded_arguments = json.loads(raw_arguments)
    except json.JSONDecodeError:
        return False
    return isinstance(decoded_arguments, Mapping)


def _retry_output(
    reason: str,
    *,
    model_execution_id: str,
    prompt_signature: str,
) -> ActiveChatRoundWorkflowOutput:
    """Return a no-consumption/no-action retry candidate for model failures."""

    return ActiveChatRoundWorkflowOutput(
        outcome=ActiveChatRoundOutcome.RETRY,
        interest_delta=0.0,
        reason=reason,
        model_execution_id=model_execution_id,
        prompt_signature=prompt_signature,
    )


def _retry_reason(value: object, *, fallback: str) -> str:
    """Return a model failure diagnostic without accepting blank text."""

    normalized = _optional_text(value)
    return normalized or fallback


def _action_reason(arguments: Mapping[str, Any], *, fallback: str) -> str:
    """Use an optional semantic reason while preserving a stable fallback."""

    normalized = _optional_text(arguments.get("reason"))
    return normalized or fallback


def _tool_call_id(raw: Mapping[str, object]) -> str:
    """Extract the model proposal identity required by the external-action ABI."""

    return _required_text(raw.get("id"), field_name="tool_call.id")


def _optional_text(value: object) -> str:
    """Normalize an optional identifier or semantic text without coercion."""

    if value is None:
        return ""
    if not isinstance(value, str):
        return ""
    return value.strip()


def _required_text(value: object, *, field_name: str) -> str:
    """Return one non-empty text value or raise a workflow boundary error."""

    if not isinstance(value, str):
        raise ActorActiveChatWorkflowError(field_name + " must be a string")
    normalized = value.strip()
    if not normalized:
        raise ActorActiveChatWorkflowError(field_name + " must not be empty")
    return normalized


def _tool_name(tool: Mapping[str, object]) -> str:
    """Read one tool schema name defensively for virtual-schema deduplication."""

    function = tool.get("function")
    if not isinstance(function, Mapping):
        return ""
    return _optional_text(function.get("name"))


def _actor_active_chat_round_tool_schema(tool: dict[str, Any]) -> dict[str, Any]:
    """Narrow chat-action schemas to the Actor v3 target grammar."""

    function = tool.get("function")
    if not isinstance(function, dict):
        return tool
    name = _optional_text(function.get("name"))
    if name not in {"no_reply", "send_reply", "send_reaction"}:
        return tool
    description = str(function.get("description") or "")
    if name == "no_reply":
        parameters = {
            "type": "object",
            "properties": {
                "intensity": {
                    "type": "string",
                    "enum": ["normal", "strong"],
                    "description": "normal or strong cooling for this round",
                },
                "reason": {"type": "string"},
            },
            "required": [],
            "additionalProperties": False,
        }
        rule = "Choose no visible response for this exact selected message batch."
    elif name == "send_reply":
        parameters = {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "quote_message_log_id": {
                    "type": "integer",
                    "description": "One selected durable message-log id to quote.",
                },
                "intensity": {
                    "type": "string",
                    "enum": ["light", "engaged"],
                },
                "reason": {"type": "string"},
            },
            "required": ["text", "quote_message_log_id"],
            "additionalProperties": False,
        }
        rule = (
            "This is the only visible action. quote_message_log_id is required "
            "and must be one of the selected message-log ids; platform ids are forbidden."
        )
    else:
        parameters = {
            "type": "object",
            "properties": {
                "emoji_id": {"type": "string"},
                "message_log_id": {
                    "type": "integer",
                    "description": "One selected durable message-log id to react to.",
                },
                "action": {"type": "string", "enum": ["add", "remove"]},
                "reason": {"type": "string"},
            },
            "required": ["emoji_id", "message_log_id"],
            "additionalProperties": False,
        }
        rule = (
            "This is the only visible action. message_log_id must be one of the "
            "selected message-log ids; platform ids are forbidden."
        )
    return {
        **tool,
        "function": {
            **function,
            "description": description + "\nActor Active Chat v3: " + rule,
            "parameters": parameters,
        },
    }


def _exit_active_tool_schema() -> dict[str, Any]:
    """Return a non-executable local control schema for fenced round exit."""

    return {
        "type": "function",
        "function": {
            "name": "exit_active",
            "description": (
                "End the active chat state after consuming this exact selected "
                "message batch. This produces no visible platform action."
            ),
            "parameters": {
                "type": "object",
                "properties": {"reason": {"type": "string"}},
                "required": ["reason"],
                "additionalProperties": False,
            },
        },
    }


__all__ = [
    "ActorActiveChatBootstrapStageInputProjector",
    "ActorActiveChatRoundPlanRunner",
    "ActorActiveChatRoundStageInputProjector",
    "ActorActiveChatWorkflowError",
    "RunnerActiveChatBootstrapWorkflow",
    "RunnerActiveChatRoundWorkflow",
    "build_actor_active_chat_bootstrap_runner",
    "build_actor_active_chat_round_plan_runner",
]
