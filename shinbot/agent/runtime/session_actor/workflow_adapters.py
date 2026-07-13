"""Actor-owned adapters for model workflow effects.

The legacy scheduler and review coordinator own mutable runtime state and may
execute visible chat-action tools.  They are intentionally not dependencies of
this module.  These adapters form the narrow actor-v2 boundary instead:

* actor-owned effect payloads provide the operation fences and delivery target;
* a ledger port supplies only the operation's captured unread input; and
* workflow ports return normalized model decisions and external-action intents.

The durable effect executor adds provenance to the completion envelope.  The
handlers below only return the versioned nested ``workflow_result`` payload, so
model code cannot forge actor-owned fences or execute a visible action before
the actor accepts its completion.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.external_actions import ExternalActionIntent
from shinbot.agent.runtime.session_actor.message_ledger import MessageLedgerEntry
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveReplyCompletionResult,
    ReviewCompletionResult,
    ReviewNextReviewOutcome,
)
from shinbot.agent.workflows.action_mode import ExternalActionToolMode

_ACTIVE_REPLY_EFFECT_KIND = "run_active_reply_workflow"
_REVIEW_EFFECT_KIND = "run_review_workflow"


class WorkflowEffectAdapterError(ValueError):
    """Raised when an actor workflow effect cannot safely be adapted."""


class ActorWorkflowLedgerPort(Protocol):
    """Read the immutable unread snapshot captured by one actor operation.

    Implementations must not return rows outside either supplied boundary.  The
    adapter repeats that check so a faulty implementation fails before a model
    can use unowned input.  This is deliberately a read-only protocol: ledger
    consumption remains an actor transition after completion acceptance.
    """

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> Sequence[MessageLedgerEntry]:
        """Return unread ledger entries visible at an operation's input fence."""


@dataclass(slots=True, frozen=True)
class ActorWorkflowEffectInput:
    """Trusted operation input shared by actor-owned workflow ports.

    ``instance_id`` and ``target_session_id`` originate in the actor effect
    payload, not a model output.  The target is the ingress base/adapter
    transport session, while ``key.session_id`` remains bot-scoped actor
    identity.  Workflow ports may use the target for prospective visible
    actions but may not replace it in their completion result.
    """

    key: SessionKey
    operation_id: str
    effect_id: str
    idempotency_key: str
    source_event_id: str
    ownership_generation: int
    instance_id: str
    target_session_id: str
    input_watermark: int
    input_ledger_sequence: int
    ledger_entries: tuple[MessageLedgerEntry, ...]

    def __post_init__(self) -> None:
        """Validate the actor-owned fence and immutable ledger snapshot."""

        for field_name in (
            "operation_id",
            "effect_id",
            "idempotency_key",
            "source_event_id",
            "instance_id",
            "target_session_id",
        ):
            normalized = _required_text(
                getattr(self, field_name),
                field_name=field_name,
            )
            object.__setattr__(self, field_name, normalized)
        _positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        _nonnegative_int(self.input_watermark, field_name="input_watermark")
        _nonnegative_int(
            self.input_ledger_sequence,
            field_name="input_ledger_sequence",
        )
        if not _is_transport_target_for_instance(
            instance_id=self.instance_id,
            target_session_id=self.target_session_id,
        ):
            raise WorkflowEffectAdapterError(
                "target_session_id must be a base transport session for instance_id"
            )
        entries = tuple(self.ledger_entries)
        previous_sequence = 0
        message_ids: set[int] = set()
        for entry in entries:
            if not isinstance(entry, MessageLedgerEntry):
                raise TypeError("ledger_entries must contain MessageLedgerEntry values")
            if entry.key != self.key:
                raise WorkflowEffectAdapterError(
                    "ledger entry belongs to a different actor session"
                )
            if entry.ledger_sequence <= previous_sequence:
                raise WorkflowEffectAdapterError(
                    "ledger entries must be strictly ordered by ledger sequence"
                )
            previous_sequence = entry.ledger_sequence
            if entry.ledger_sequence > self.input_ledger_sequence:
                raise WorkflowEffectAdapterError(
                    "ledger entry exceeds the captured input_ledger_sequence"
                )
            if entry.message_log_id > self.input_watermark:
                raise WorkflowEffectAdapterError(
                    "ledger entry exceeds the captured input_watermark"
                )
            if entry.message_log_id in message_ids:
                raise WorkflowEffectAdapterError(
                    "captured ledger entries contain a duplicate message_log_id"
                )
            message_ids.add(entry.message_log_id)
            if not entry.is_unread:
                raise WorkflowEffectAdapterError(
                    "captured ledger entries must still be unread"
                )
        object.__setattr__(self, "ledger_entries", entries)

    @property
    def message_log_ids(self) -> tuple[int, ...]:
        """Return captured message identities in durable ledger order."""

        return tuple(entry.message_log_id for entry in self.ledger_entries)


@dataclass(slots=True, frozen=True)
class ActiveReplyWorkflowRequest:
    """One actor-owned active-reply model invocation request."""

    effect: ActorWorkflowEffectInput
    message_log_ids: tuple[int, ...]
    response_profile: str = ""
    sender_id: str = ""
    external_action_mode: ExternalActionToolMode = (
        ExternalActionToolMode.COLLECT_INTENTS
    )

    def __post_init__(self) -> None:
        """Require exactly the captured active-reply message selection."""

        if not isinstance(self.effect, ActorWorkflowEffectInput):
            raise TypeError("effect must be ActorWorkflowEffectInput")
        if ExternalActionToolMode(self.external_action_mode) is not (
            ExternalActionToolMode.COLLECT_INTENTS
        ):
            raise WorkflowEffectAdapterError(
                "actor workflows must collect external action intents"
            )
        message_ids = _positive_id_tuple(
            self.message_log_ids,
            field_name="message_log_ids",
        )
        captured_ids = set(self.effect.message_log_ids)
        if any(message_id not in captured_ids for message_id in message_ids):
            raise WorkflowEffectAdapterError(
                "active reply input includes a message outside its captured ledger"
            )
        object.__setattr__(self, "message_log_ids", message_ids)
        object.__setattr__(
            self,
            "response_profile",
            _optional_text(self.response_profile, field_name="response_profile"),
        )
        object.__setattr__(
            self,
            "sender_id",
            _optional_text(self.sender_id, field_name="sender_id"),
        )


@dataclass(slots=True, frozen=True)
class ReviewWorkflowRequest:
    """One actor-owned review model invocation request."""

    effect: ActorWorkflowEffectInput
    review_plan: dict[str, Any]
    plan_id: str
    plan_revision: int
    external_action_mode: ExternalActionToolMode = (
        ExternalActionToolMode.COLLECT_INTENTS
    )

    def __post_init__(self) -> None:
        """Validate the actor-owned plan and force intent collection mode."""

        if not isinstance(self.effect, ActorWorkflowEffectInput):
            raise TypeError("effect must be ActorWorkflowEffectInput")
        if ExternalActionToolMode(self.external_action_mode) is not (
            ExternalActionToolMode.COLLECT_INTENTS
        ):
            raise WorkflowEffectAdapterError(
                "actor workflows must collect external action intents"
            )
        plan_id = _required_text(self.plan_id, field_name="plan_id")
        plan_revision = _positive_int(self.plan_revision, field_name="plan_revision")
        review_plan = _plain_json_object(self.review_plan, field_name="review_plan")
        if _required_text(
            review_plan.get("plan_id"),
            field_name="review_plan.plan_id",
        ) != plan_id:
            raise WorkflowEffectAdapterError("review_plan.plan_id changed the effect plan")
        if _positive_int(
            review_plan.get("plan_revision"),
            field_name="review_plan.plan_revision",
        ) != plan_revision:
            raise WorkflowEffectAdapterError(
                "review_plan.plan_revision changed the effect plan"
            )
        object.__setattr__(self, "plan_id", plan_id)
        object.__setattr__(self, "plan_revision", plan_revision)
        object.__setattr__(self, "review_plan", review_plan)


@dataclass(slots=True, frozen=True)
class ActiveReplyWorkflowOutput:
    """Pure active-reply model result before actor completion encoding."""

    consumed_message_log_ids: tuple[int, ...] = ()
    external_action_intents: tuple[ExternalActionIntent, ...] = ()

    def __post_init__(self) -> None:
        """Detach caller-owned sequences without assigning actor provenance."""

        object.__setattr__(
            self,
            "consumed_message_log_ids",
            _positive_id_tuple(
                self.consumed_message_log_ids,
                field_name="consumed_message_log_ids",
            ),
        )
        object.__setattr__(
            self,
            "external_action_intents",
            tuple(self.external_action_intents),
        )


@dataclass(slots=True, frozen=True)
class ReviewWorkflowWindowOutput:
    """One review reply window's model decisions and local action proposals.

    Local tool-call identifiers and ordinals are valid only within this window.
    :class:`ReviewWorkflowEffectHandler` converts them into operation-global
    proposal identities and ordinals before encoding the completion.
    """

    window_id: str
    consumed_message_log_ids: tuple[int, ...] = ()
    external_action_intents: tuple[ExternalActionIntent, ...] = ()

    def __post_init__(self) -> None:
        """Normalize the local window identity and ordered output sequences."""

        object.__setattr__(
            self,
            "window_id",
            _required_text(self.window_id, field_name="window_id"),
        )
        object.__setattr__(
            self,
            "consumed_message_log_ids",
            _positive_id_tuple(
                self.consumed_message_log_ids,
                field_name="consumed_message_log_ids",
            ),
        )
        object.__setattr__(
            self,
            "external_action_intents",
            tuple(self.external_action_intents),
        )


@dataclass(slots=True, frozen=True)
class ReviewWorkflowOutput:
    """Pure review model result before actor completion encoding."""

    enter_active_chat: bool
    next_review_outcome: ReviewNextReviewOutcome | None
    consumed_message_log_ids: tuple[int, ...] = ()
    reply_windows: tuple[ReviewWorkflowWindowOutput, ...] = ()
    model_execution_id: str = ""
    prompt_signature: str = ""

    def __post_init__(self) -> None:
        """Normalize caller-owned output sequences without choosing a schedule."""

        if not isinstance(self.enter_active_chat, bool):
            raise TypeError("enter_active_chat must be a boolean")
        if self.next_review_outcome is not None and not isinstance(
            self.next_review_outcome,
            ReviewNextReviewOutcome,
        ):
            raise TypeError("next_review_outcome must be ReviewNextReviewOutcome or None")
        object.__setattr__(
            self,
            "consumed_message_log_ids",
            _positive_id_tuple(
                self.consumed_message_log_ids,
                field_name="consumed_message_log_ids",
            ),
        )
        windows = tuple(self.reply_windows)
        if any(not isinstance(window, ReviewWorkflowWindowOutput) for window in windows):
            raise TypeError("reply_windows must contain ReviewWorkflowWindowOutput values")
        object.__setattr__(self, "reply_windows", windows)
        object.__setattr__(
            self,
            "model_execution_id",
            _optional_text(
                self.model_execution_id,
                field_name="model_execution_id",
            ),
        )
        object.__setattr__(
            self,
            "prompt_signature",
            _optional_text(
                self.prompt_signature,
                field_name="prompt_signature",
            ),
        )


class ActiveReplyWorkflowPort(Protocol):
    """Model-facing active-reply workflow with no scheduler or tool executor."""

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Return a decision and deferred external action intents."""


class ReviewWorkflowPort(Protocol):
    """Model-facing review workflow with no scheduler or tool executor."""

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Return review decisions and per-window deferred action intents."""


class ActiveReplyWorkflowEffectHandler:
    """Adapt one active-reply effect into a strict mailbox completion payload."""

    def __init__(
        self,
        *,
        ledger: ActorWorkflowLedgerPort,
        workflow: ActiveReplyWorkflowPort,
    ) -> None:
        self._ledger = ledger
        self._workflow = workflow

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Run the pure workflow and return its versioned nested completion."""

        metadata = _effect_metadata(context, expected_kind=_ACTIVE_REPLY_EFFECT_KIND)
        captured = await _load_captured_unread(self._ledger, metadata)
        message_log_ids = _positive_id_tuple(
            context.effect.payload.get("message_log_ids"),
            field_name="message_log_ids",
        )
        if not message_log_ids:
            raise WorkflowEffectAdapterError(
                "active reply effect must reference at least one captured message"
            )
        entries_by_id = {entry.message_log_id: entry for entry in captured}
        missing = [
            message_log_id
            for message_log_id in message_log_ids
            if message_log_id not in entries_by_id
        ]
        if missing:
            raise WorkflowEffectAdapterError(
                "active reply effect references messages outside its captured ledger: "
                + ", ".join(str(message_log_id) for message_log_id in missing)
            )
        selected_message_ids = set(message_log_ids)
        selected_entries = tuple(
            entry
            for entry in captured
            if entry.message_log_id in selected_message_ids
        )
        request = ActiveReplyWorkflowRequest(
            effect=metadata.with_entries(selected_entries),
            message_log_ids=message_log_ids,
            response_profile=_optional_text(
                context.effect.payload.get("response_profile"),
                field_name="response_profile",
            ),
            sender_id=_optional_text(
                context.effect.payload.get("sender_id"),
                field_name="sender_id",
            ),
            external_action_mode=ExternalActionToolMode.COLLECT_INTENTS,
        )
        output = await self._workflow.run_active_reply(request)
        if not isinstance(output, ActiveReplyWorkflowOutput):
            raise TypeError("active reply workflow returned an invalid output type")
        _validate_consumed_ids(
            output.consumed_message_log_ids,
            allowed_message_log_ids=set(request.message_log_ids),
            operation_name="active reply",
        )
        completion = ActiveReplyCompletionResult(
            consumed_message_log_ids=output.consumed_message_log_ids,
            external_action_intents=output.external_action_intents,
        )
        return EffectHandlerResult(payload={"workflow_result": completion.to_payload()})


class ReviewWorkflowEffectHandler:
    """Adapt one review effect into a strict mailbox completion payload."""

    def __init__(
        self,
        *,
        ledger: ActorWorkflowLedgerPort,
        workflow: ReviewWorkflowPort,
    ) -> None:
        self._ledger = ledger
        self._workflow = workflow

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Run pure review work and make all window proposals operation-global."""

        metadata = _effect_metadata(context, expected_kind=_REVIEW_EFFECT_KIND)
        payload = context.effect.payload
        plan_id = _required_text(payload.get("plan_id"), field_name="plan_id")
        plan_revision = _positive_int(
            payload.get("plan_revision"),
            field_name="plan_revision",
        )
        review_plan = _plain_json_object(payload.get("review_plan"), field_name="review_plan")
        captured = await _load_captured_unread(self._ledger, metadata)
        request = ReviewWorkflowRequest(
            effect=metadata.with_entries(captured),
            review_plan=review_plan,
            plan_id=plan_id,
            plan_revision=plan_revision,
            external_action_mode=ExternalActionToolMode.COLLECT_INTENTS,
        )
        output = await self._workflow.run_review(request)
        if not isinstance(output, ReviewWorkflowOutput):
            raise TypeError("review workflow returned an invalid output type")
        consumed_message_log_ids = _review_consumed_ids(output)
        _validate_consumed_ids(
            consumed_message_log_ids,
            allowed_message_log_ids=set(request.effect.message_log_ids),
            operation_name="review",
        )
        completion = ReviewCompletionResult(
            enter_active_chat=output.enter_active_chat,
            next_review_outcome=output.next_review_outcome,
            consumed_message_log_ids=consumed_message_log_ids,
            external_action_intents=_operation_global_review_intents(
                operation_id=request.effect.operation_id,
                windows=output.reply_windows,
            ),
        )
        return EffectHandlerResult(
            payload={
                "workflow_result": completion.to_payload(),
                "model_execution_id": output.model_execution_id,
                "prompt_signature": output.prompt_signature,
            }
        )


def register_actor_workflow_effect_handlers(
    registry: EffectHandlerRegistry,
    *,
    ledger: ActorWorkflowLedgerPort,
    active_reply_workflow: ActiveReplyWorkflowPort,
    review_workflow: ReviewWorkflowPort,
) -> tuple[ActiveReplyWorkflowEffectHandler, ReviewWorkflowEffectHandler]:
    """Register the two actor-v2 workflow handlers on an explicit registry.

    Runtime activation deliberately remains outside this module.  The caller
    can create these handlers only after its ownership/activation gate has
    chosen actor-v2 traffic, then register them with the durable effect
    executor's registry.
    """

    active_reply_handler = ActiveReplyWorkflowEffectHandler(
        ledger=ledger,
        workflow=active_reply_workflow,
    )
    review_handler = ReviewWorkflowEffectHandler(
        ledger=ledger,
        workflow=review_workflow,
    )
    active_reply_contracts = tuple(
        contract
        for contract in builtin_session_actor_effect_contracts()
        if contract.effect_kind == _ACTIVE_REPLY_EFFECT_KIND
    )
    review_contracts = tuple(
        contract
        for contract in builtin_session_actor_effect_contracts()
        if contract.effect_kind == _REVIEW_EFFECT_KIND
    )
    for contract in active_reply_contracts:
        registry.register(
            _ACTIVE_REPLY_EFFECT_KIND,
            active_reply_handler,
            contract=contract,
        )
    for contract in review_contracts:
        registry.register(
            _REVIEW_EFFECT_KIND,
            review_handler,
            contract=contract,
        )
    return active_reply_handler, review_handler


@dataclass(slots=True, frozen=True)
class _EffectMetadata:
    """Validated actor-owned fields copied from one durable effect envelope."""

    key: SessionKey
    operation_id: str
    effect_id: str
    idempotency_key: str
    source_event_id: str
    ownership_generation: int
    instance_id: str
    target_session_id: str
    input_watermark: int
    input_ledger_sequence: int

    def with_entries(
        self,
        entries: Sequence[MessageLedgerEntry],
    ) -> ActorWorkflowEffectInput:
        """Bind one validated immutable ledger snapshot to this effect metadata."""

        return ActorWorkflowEffectInput(
            key=self.key,
            operation_id=self.operation_id,
            effect_id=self.effect_id,
            idempotency_key=self.idempotency_key,
            source_event_id=self.source_event_id,
            ownership_generation=self.ownership_generation,
            instance_id=self.instance_id,
            target_session_id=self.target_session_id,
            input_watermark=self.input_watermark,
            input_ledger_sequence=self.input_ledger_sequence,
            ledger_entries=tuple(entries),
        )


def _effect_metadata(
    context: EffectExecutionContext,
    *,
    expected_kind: str,
) -> _EffectMetadata:
    """Validate actor-owned effect data before any workflow/model call."""

    effect = context.effect
    if effect.kind != expected_kind:
        raise WorkflowEffectAdapterError(
            f"workflow adapter expected {expected_kind!r}, got {effect.kind!r}"
        )
    if effect.ownership_generation < 1:
        raise WorkflowEffectAdapterError(
            "actor workflow effects require a positive ownership generation"
        )
    payload = effect.payload
    if not isinstance(payload, Mapping):
        raise WorkflowEffectAdapterError("workflow effect payload must be an object")

    operation_id = _required_text(payload.get("operation_id"), field_name="operation_id")
    effect_id = _required_text(payload.get("effect_id"), field_name="effect_id")
    idempotency_key = _required_text(
        payload.get("idempotency_key"),
        field_name="idempotency_key",
    )
    source_event_id = _required_text(
        payload.get("source_event_id"),
        field_name="source_event_id",
    )
    effect_kind = _required_text(payload.get("effect_kind"), field_name="effect_kind")
    ownership_generation = _positive_int(
        payload.get("ownership_generation"),
        field_name="ownership_generation",
    )
    input_watermark = _nonnegative_int(
        payload.get("input_watermark"),
        field_name="input_watermark",
    )
    input_ledger_sequence = _nonnegative_int(
        payload.get("input_ledger_sequence"),
        field_name="input_ledger_sequence",
    )
    instance_id = _required_text(payload.get("instance_id"), field_name="instance_id")
    target_session_id = _required_text(
        payload.get("target_session_id"),
        field_name="target_session_id",
    )

    expected = {
        "operation_id": effect.operation_id,
        "effect_id": effect.effect_id,
        "idempotency_key": effect.idempotency_key,
        "source_event_id": effect.source_event_id,
        "effect_kind": expected_kind,
    }
    actual = {
        "operation_id": operation_id,
        "effect_id": effect_id,
        "idempotency_key": idempotency_key,
        "source_event_id": source_event_id,
        "effect_kind": effect_kind,
    }
    changed = [name for name, value in actual.items() if value != expected[name]]
    if changed:
        raise WorkflowEffectAdapterError(
            "workflow effect payload changed durable identity: " + ", ".join(changed)
        )
    if ownership_generation != effect.ownership_generation:
        raise WorkflowEffectAdapterError(
            "workflow effect payload changed ownership_generation"
        )
    if not _is_transport_target_for_instance(
        instance_id=instance_id,
        target_session_id=target_session_id,
    ):
        raise WorkflowEffectAdapterError(
            "workflow effect target_session_id is not owned by instance_id"
        )
    return _EffectMetadata(
        key=effect.key,
        operation_id=operation_id,
        effect_id=effect_id,
        idempotency_key=idempotency_key,
        source_event_id=source_event_id,
        ownership_generation=ownership_generation,
        instance_id=instance_id,
        target_session_id=target_session_id,
        input_watermark=input_watermark,
        input_ledger_sequence=input_ledger_sequence,
    )


async def _load_captured_unread(
    ledger: ActorWorkflowLedgerPort,
    metadata: _EffectMetadata,
) -> tuple[MessageLedgerEntry, ...]:
    """Read and revalidate exactly the snapshot captured by the operation."""

    entries = await ledger.list_captured_unread(
        key=metadata.key,
        input_watermark=metadata.input_watermark,
        input_ledger_sequence=metadata.input_ledger_sequence,
    )
    return metadata.with_entries(entries).ledger_entries


def _review_consumed_ids(output: ReviewWorkflowOutput) -> tuple[int, ...]:
    """Flatten review-level and per-window consumption in deterministic order."""

    consumed: list[int] = list(output.consumed_message_log_ids)
    seen = set(consumed)
    window_ids: set[str] = set()
    for window in output.reply_windows:
        if window.window_id in window_ids:
            raise WorkflowEffectAdapterError(
                f"duplicate review reply window_id: {window.window_id!r}"
            )
        window_ids.add(window.window_id)
        for message_log_id in window.consumed_message_log_ids:
            if message_log_id in seen:
                raise WorkflowEffectAdapterError(
                    "review workflow consumed one message more than once: "
                    f"{message_log_id}"
                )
            seen.add(message_log_id)
            consumed.append(message_log_id)
    return tuple(consumed)


def _operation_global_review_intents(
    *,
    operation_id: str,
    windows: Sequence[ReviewWorkflowWindowOutput],
) -> tuple[ExternalActionIntent, ...]:
    """Make local review-window proposals operation-global and deterministic."""

    normalized_operation_id = _required_text(operation_id, field_name="operation_id")
    result: list[ExternalActionIntent] = []
    seen_window_ids: set[str] = set()
    seen_proposal_ids: set[str] = set()
    for window in windows:
        if not isinstance(window, ReviewWorkflowWindowOutput):
            raise TypeError("reply_windows must contain ReviewWorkflowWindowOutput values")
        if window.window_id in seen_window_ids:
            raise WorkflowEffectAdapterError(
                f"duplicate review reply window_id: {window.window_id!r}"
            )
        seen_window_ids.add(window.window_id)
        local_ids: set[str] = set()
        for local_ordinal, intent in enumerate(window.external_action_intents):
            if not isinstance(intent, ExternalActionIntent):
                raise TypeError("review window intents must be ExternalActionIntent values")
            if intent.action_ordinal != local_ordinal:
                raise WorkflowEffectAdapterError(
                    "review window action ordinals must be contiguous from zero"
                )
            raw_proposal_id = _required_text(
                intent.tool_call_id,
                field_name="review window proposal_id",
            )
            if raw_proposal_id in local_ids:
                raise WorkflowEffectAdapterError(
                    "review window contains a duplicate proposal_id: "
                    f"{raw_proposal_id!r}"
                )
            local_ids.add(raw_proposal_id)
            proposal_id = operation_global_review_proposal_id(
                operation_id=normalized_operation_id,
                window_id=window.window_id,
                local_proposal_id=raw_proposal_id,
            )
            if proposal_id in seen_proposal_ids:
                raise WorkflowEffectAdapterError(
                    "review workflow generated a duplicate operation proposal_id: "
                    f"{proposal_id!r}"
                )
            seen_proposal_ids.add(proposal_id)
            result.append(
                ExternalActionIntent(
                    kind=intent.kind,
                    tool_call_id=proposal_id,
                    action_ordinal=len(result),
                    payload=dict(intent.payload),
                )
            )
    return tuple(result)


def operation_global_review_proposal_id(
    *,
    operation_id: str,
    window_id: str,
    local_proposal_id: str,
) -> str:
    """Return an unambiguous stable proposal id for one review window action.

    Length-prefixing avoids collisions when model-provided tool ids or window
    ids contain separators.  The operation identity is part of every id, so
    actions from two review operations can never share a logical action slot.
    """

    normalized_operation_id = _required_text(operation_id, field_name="operation_id")
    normalized_window_id = _required_text(window_id, field_name="window_id")
    normalized_local_id = _required_text(
        local_proposal_id,
        field_name="local_proposal_id",
    )
    return (
        "review-proposal:"
        f"{normalized_operation_id}:"
        f"{len(normalized_window_id)}:{normalized_window_id}:"
        f"{len(normalized_local_id)}:{normalized_local_id}"
    )


def _validate_consumed_ids(
    message_log_ids: Sequence[int],
    *,
    allowed_message_log_ids: set[int],
    operation_name: str,
) -> None:
    """Reject workflow attempts to consume messages outside its snapshot."""

    outside = [
        message_log_id
        for message_log_id in message_log_ids
        if message_log_id not in allowed_message_log_ids
    ]
    if outside:
        raise WorkflowEffectAdapterError(
            f"{operation_name} workflow consumed messages outside its captured ledger: "
            + ", ".join(str(message_log_id) for message_log_id in outside)
        )


def _is_transport_target_for_instance(
    *,
    instance_id: str,
    target_session_id: str,
) -> bool:
    """Return whether a base session belongs to its persisted adapter instance."""

    return target_session_id.startswith(f"{instance_id}:")


def _required_text(value: object, *, field_name: str) -> str:
    """Return one non-empty text value or raise a boundary error."""

    if not isinstance(value, str):
        raise WorkflowEffectAdapterError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise WorkflowEffectAdapterError(f"{field_name} must not be empty")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    """Normalize optional text without truthy coercion."""

    if value is None:
        return ""
    if not isinstance(value, str):
        raise WorkflowEffectAdapterError(f"{field_name} must be a string")
    return value.strip()


def _positive_int(value: object, *, field_name: str) -> int:
    """Return one positive integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WorkflowEffectAdapterError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    """Return one non-negative integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WorkflowEffectAdapterError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _positive_id_tuple(value: object, *, field_name: str) -> tuple[int, ...]:
    """Normalize a duplicate-free sequence of positive message identifiers."""

    if not isinstance(value, (list, tuple)):
        raise WorkflowEffectAdapterError(f"{field_name} must be an array")
    result: list[int] = []
    seen: set[int] = set()
    for index, item in enumerate(value):
        message_log_id = _positive_int(item, field_name=f"{field_name}[{index}]")
        if message_log_id in seen:
            raise WorkflowEffectAdapterError(
                f"{field_name} contains a duplicate message_log_id: {message_log_id}"
            )
        seen.add(message_log_id)
        result.append(message_log_id)
    return tuple(result)


def _plain_json_object(value: object, *, field_name: str) -> dict[str, Any]:
    """Detach a strict JSON-compatible object from a durable effect payload."""

    if not isinstance(value, Mapping):
        raise WorkflowEffectAdapterError(f"{field_name} must be an object")
    return {
        _required_text(key, field_name=f"{field_name} key"): _plain_json_value(
            item,
            field_name=f"{field_name}.{key}",
        )
        for key, item in value.items()
    }


def _plain_json_value(value: object, *, field_name: str) -> Any:
    """Copy one finite JSON-compatible value without retaining frozen payloads."""

    if isinstance(value, Mapping):
        return _plain_json_object(value, field_name=field_name)
    if isinstance(value, (list, tuple)):
        return [
            _plain_json_value(item, field_name=f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, float) and not math.isfinite(value):
        raise WorkflowEffectAdapterError(f"{field_name} numbers must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise WorkflowEffectAdapterError(
        f"{field_name} must contain only JSON-compatible values"
    )


__all__ = [
    "ActiveReplyWorkflowEffectHandler",
    "ActiveReplyWorkflowOutput",
    "ActiveReplyWorkflowPort",
    "ActiveReplyWorkflowRequest",
    "ActorWorkflowEffectInput",
    "ActorWorkflowLedgerPort",
    "ReviewWorkflowEffectHandler",
    "ReviewWorkflowOutput",
    "ReviewWorkflowPort",
    "ReviewWorkflowRequest",
    "ReviewWorkflowWindowOutput",
    "WorkflowEffectAdapterError",
    "operation_global_review_proposal_id",
    "register_actor_workflow_effect_handlers",
]
