"""Read-only durable context projection for Actor v3 Active Chat effects.

Actor-native Active Chat deliberately does not borrow the legacy coordinator's
tail history, summaries, pending buffers, or in-memory conversation state.
Bootstrap receives only the exact messages durably consumed by the review
operation that created its handoff.  Each later round reads the operation's
dual-fenced unread snapshot and exposes only its explicit selection.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Protocol

from shinbot.agent.coordinators.review.stores import MessageLogPayload
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageConsumptionProvenance,
    MessageLedgerEntry,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveChatBootstrapWorkflowRequest,
    ActiveChatRoundWorkflowRequest,
    ActorWorkflowEffectInput,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapDisposition,
)
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)
from shinbot.agent.workflows.action_mode import ExternalActionToolMode

_BOOTSTRAP_STAGE_PURPOSE = "active_chat_bootstrap"
_ROUND_STAGE_PURPOSE = "active_chat_round"


class ActorActiveChatWorkflowContextError(RuntimeError):
    """Raised when an Actor v3 Active Chat context cannot be projected safely."""


class ActorActiveChatBootstrapLedgerPort(Protocol):
    """Read the full durable ledger needed to prove a review handoff.

    Bootstrap input rows have already been consumed by review, so they cannot
    appear in the unread projection.  Implementations return metadata-only
    ledger rows in durable sequence order and must not mutate any ledger state.
    """

    async def list_message_ledger(
        self,
        key: SessionKey,
    ) -> Sequence[MessageLedgerEntry]:
        """Return one complete actor ledger in durable sequence order."""


class ActorActiveChatRoundLedgerPort(Protocol):
    """Read an exact unread projection for one operation's frozen boundary."""

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        ownership_generation: int,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> Sequence[MessageLedgerEntry]:
        """Return unread rows visible through both supplied durable fences."""


class ActorActiveChatWorkflowMessageStore(Protocol):
    """Load immutable message logs only after the actor authorizes their IDs."""

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[MessageLogPayload]:
        """Return existing message logs for the supplied identifiers."""


class ActorActiveChatBootstrapStageInputProjector(Protocol):
    """Project the review-consumed handoff used by one bootstrap decision."""

    async def build_active_chat_bootstrap_stage_input(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ReviewStageInput:
        """Return one exact bootstrap stage input without live history."""


class ActorActiveChatRoundStageInputProjector(Protocol):
    """Project one frozen unread selection used by an Active Chat round."""

    async def build_active_chat_round_stage_input(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ReviewStageInput:
        """Return one exact round stage input without coordinator state."""


class _ActorActiveChatWorkflowContextProjectorBase:
    """Shared immutable message loading and stage-input validation helpers."""

    def __init__(
        self,
        *,
        message_store: ActorActiveChatWorkflowMessageStore,
        context_builder: ReviewContextBuilder | None = None,
    ) -> None:
        self._message_store = message_store
        self._context_builder = context_builder or ReviewContextBuilderAdapter()

    def _load_messages(
        self,
        entries: tuple[MessageLedgerEntry, ...],
        *,
        effect: ActorWorkflowEffectInput,
        operation_name: str,
    ) -> list[MessageLogPayload]:
        """Reload exactly selected message logs and restore durable ledger order."""

        message_log_ids = tuple(entry.message_log_id for entry in entries)
        if not message_log_ids:
            return []
        expected_session_ids = {
            entry.message_log_id: entry.message.base_session_id for entry in entries
        }
        payloads = self._message_store.list_by_ids(message_log_ids)
        if not isinstance(payloads, list):
            raise ActorActiveChatWorkflowContextError(
                f"actor active chat {operation_name} message store returned an invalid payload list"
            )
        by_id: dict[int, MessageLogPayload] = {}
        expected_ids = set(message_log_ids)
        for payload in payloads:
            if not isinstance(payload, Mapping):
                raise ActorActiveChatWorkflowContextError(
                    f"actor active chat {operation_name} message store returned an invalid payload"
                )
            message_log_id = payload.get("id")
            if (
                isinstance(message_log_id, bool)
                or not isinstance(message_log_id, int)
                or message_log_id not in expected_ids
            ):
                raise ActorActiveChatWorkflowContextError(
                    f"actor active chat {operation_name} message store returned an unexpected message"
                )
            if message_log_id in by_id:
                raise ActorActiveChatWorkflowContextError(
                    f"actor active chat {operation_name} message store returned a duplicate message"
                )
            session_id = payload.get("session_id")
            if (
                not isinstance(session_id, str)
                or session_id != expected_session_ids[message_log_id]
                or session_id != effect.target_session_id
            ):
                raise ActorActiveChatWorkflowContextError(
                    "actor active chat "
                    + operation_name
                    + " message log session mismatch: "
                    + str(message_log_id)
                )
            by_id[message_log_id] = dict(payload)
        missing = [
            message_log_id
            for message_log_id in message_log_ids
            if message_log_id not in by_id
        ]
        if missing:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat "
                + operation_name
                + " message logs disappeared: "
                + ", ".join(str(message_log_id) for message_log_id in missing)
            )
        return [by_id[message_log_id] for message_log_id in message_log_ids]

    def _build_stage_input(
        self,
        *,
        effect: ActorWorkflowEffectInput,
        messages: list[MessageLogPayload],
        purpose: str,
        metadata: dict[str, object],
        operation_name: str,
    ) -> ReviewStageInput:
        """Build and verify a stage input containing no hidden runtime context."""

        expected_metadata = {"purpose": purpose, **metadata}
        stage_input = self._context_builder.build_for_messages(
            session_id=effect.key.session_id,
            messages=messages,
            purpose=purpose,
            options=ReviewContextBuildOptions(
                instance_id=effect.instance_id,
                metadata=metadata,
            ),
        )
        _validate_projected_stage_input(
            stage_input,
            effect=effect,
            messages=messages,
            purpose=purpose,
            expected_metadata=expected_metadata,
            operation_name=operation_name,
        )
        return stage_input


class ActorActiveChatBootstrapWorkflowContextProjector(
    _ActorActiveChatWorkflowContextProjectorBase,
):
    """Build a bootstrap input from exactly one persisted review handoff.

    The projector reads the full ledger only because review-consumed rows are
    excluded from unread projections.  It never reads message history by time,
    tail window, summary, or coordinator state; only the handoff IDs may reach
    the message-log store and the model-facing stage input.
    """

    def __init__(
        self,
        *,
        ledger: ActorActiveChatBootstrapLedgerPort,
        message_store: ActorActiveChatWorkflowMessageStore,
        context_builder: ReviewContextBuilder | None = None,
    ) -> None:
        super().__init__(message_store=message_store, context_builder=context_builder)
        self._ledger = ledger

    async def build_active_chat_bootstrap_stage_input(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ReviewStageInput:
        """Project exactly the review-consumed bootstrap handoff in ledger order."""

        if not isinstance(request, ActiveChatBootstrapWorkflowRequest):
            raise TypeError("request must be ActiveChatBootstrapWorkflowRequest")
        effect = _validated_effect(request.effect, operation_name="bootstrap")
        active_epoch = _positive_int(
            request.active_epoch,
            field_name="active_chat_bootstrap.active_epoch",
        )
        handoff_operation_id = _required_text(
            request.handoff_operation_id,
            field_name="active_chat_bootstrap.handoff_operation_id",
        )
        handoff_message_log_ids = _positive_id_tuple(
            request.handoff_message_log_ids,
            field_name="active_chat_bootstrap.handoff_message_log_ids",
        )
        if effect.ledger_entries != ():
            raise ActorActiveChatWorkflowContextError(
                "actor active chat bootstrap effect must not carry unread ledger entries"
            )

        entries = await self._ledger.list_message_ledger(effect.key)
        selected_entries = _select_bootstrap_handoff_entries(
            entries,
            effect=effect,
            handoff_operation_id=handoff_operation_id,
            handoff_message_log_ids=handoff_message_log_ids,
        )
        messages = self._load_messages(
            selected_entries,
            effect=effect,
            operation_name="bootstrap",
        )
        selected_message_log_ids = tuple(
            entry.message_log_id for entry in selected_entries
        )
        return self._build_stage_input(
            effect=effect,
            messages=messages,
            purpose=_BOOTSTRAP_STAGE_PURPOSE,
            metadata={
                "actor_v2": True,
                "active_chat_v3": True,
                "operation_id": effect.operation_id,
                "effect_id": effect.effect_id,
                "ownership_generation": effect.ownership_generation,
                "input_watermark": effect.input_watermark,
                "input_ledger_sequence": effect.input_ledger_sequence,
                "target_session_id": effect.target_session_id,
                "ledger_message_log_ids": list(selected_message_log_ids),
                "candidate_message_ids": list(selected_message_log_ids),
                "active_epoch": active_epoch,
                "handoff_operation_id": handoff_operation_id,
                "handoff_message_log_ids": list(selected_message_log_ids),
            },
            operation_name="bootstrap",
        )


class ActorActiveChatRoundWorkflowContextProjector(
    _ActorActiveChatWorkflowContextProjectorBase,
):
    """Build one Active Chat round input from an exact frozen unread selection."""

    def __init__(
        self,
        *,
        ledger: ActorActiveChatRoundLedgerPort,
        message_store: ActorActiveChatWorkflowMessageStore,
        context_builder: ReviewContextBuilder | None = None,
    ) -> None:
        super().__init__(message_store=message_store, context_builder=context_builder)
        self._ledger = ledger

    async def build_active_chat_round_stage_input(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ReviewStageInput:
        """Project only the selected unread rows from the round's dual fence."""

        if not isinstance(request, ActiveChatRoundWorkflowRequest):
            raise TypeError("request must be ActiveChatRoundWorkflowRequest")
        effect = _validated_effect(request.effect, operation_name="round")
        active_epoch = _positive_int(
            request.active_epoch,
            field_name="active_chat_round.active_epoch",
        )
        round_schedule_id = _required_text(
            request.round_schedule_id,
            field_name="active_chat_round.round_schedule_id",
        )
        message_log_ids = _positive_id_tuple(
            request.message_log_ids,
            field_name="active_chat_round.message_log_ids",
        )
        if not message_log_ids:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round requires at least one selected message"
            )
        interest_value = _nonnegative_finite_number(
            request.interest_value,
            field_name="active_chat_round.interest_value",
        )
        bootstrap_disposition = _bootstrap_disposition(
            request.bootstrap_disposition,
            field_name="active_chat_round.bootstrap_disposition",
        )
        try:
            action_mode = ExternalActionToolMode(request.external_action_mode)
        except (TypeError, ValueError) as exc:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round external_action_mode is invalid"
            ) from exc
        if action_mode is not ExternalActionToolMode.COLLECT_INTENTS:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round must collect external action intents"
            )

        snapshot_entries = _validated_round_snapshot_entries(effect)
        snapshot_message_log_ids = tuple(
            entry.message_log_id for entry in snapshot_entries
        )
        if (
            len(snapshot_message_log_ids) != len(message_log_ids)
            or set(snapshot_message_log_ids) != set(message_log_ids)
        ):
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round request message ids do not match its selected "
                "ledger entries"
            )

        captured_entries = await self._ledger.list_captured_unread(
            key=effect.key,
            ownership_generation=effect.ownership_generation,
            input_watermark=effect.input_watermark,
            input_ledger_sequence=effect.input_ledger_sequence,
        )
        captured_entries = _validated_round_captured_entries(
            captured_entries,
            effect=effect,
        )
        selected_entries = _select_round_entries(
            captured_entries,
            effect=effect,
            snapshot_entries=snapshot_entries,
            selected_message_log_ids=message_log_ids,
        )
        messages = self._load_messages(
            selected_entries,
            effect=effect,
            operation_name="round",
        )
        selected_ids_in_ledger_order = tuple(
            entry.message_log_id for entry in selected_entries
        )
        return self._build_stage_input(
            effect=effect,
            messages=messages,
            purpose=_ROUND_STAGE_PURPOSE,
            metadata={
                "actor_v2": True,
                "active_chat_v3": True,
                "operation_id": effect.operation_id,
                "effect_id": effect.effect_id,
                "ownership_generation": effect.ownership_generation,
                "input_watermark": effect.input_watermark,
                "input_ledger_sequence": effect.input_ledger_sequence,
                "target_session_id": effect.target_session_id,
                "ledger_message_log_ids": list(selected_ids_in_ledger_order),
                "candidate_message_ids": list(selected_ids_in_ledger_order),
                "active_epoch": active_epoch,
                "round_schedule_id": round_schedule_id,
                "interest_value": interest_value,
                "active_chat_interest_value": interest_value,
                "bootstrap_disposition": bootstrap_disposition,
                "message_log_ids": list(selected_ids_in_ledger_order),
            },
            operation_name="round",
        )


def _validated_effect(
    effect: object,
    *,
    operation_name: str,
) -> ActorWorkflowEffectInput:
    """Revalidate an effect that may have crossed an untrusted workflow boundary."""

    if not isinstance(effect, ActorWorkflowEffectInput):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} request contains an invalid effect"
        )
    if not isinstance(effect.key, SessionKey):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} effect contains an invalid session key"
        )
    for field_name in (
        "operation_id",
        "effect_id",
        "idempotency_key",
        "source_event_id",
        "instance_id",
        "target_session_id",
    ):
        _required_text(
            getattr(effect, field_name),
            field_name=f"active_chat_{operation_name}.{field_name}",
        )
    _positive_int(
        effect.ownership_generation,
        field_name=f"active_chat_{operation_name}.ownership_generation",
    )
    _nonnegative_int(
        effect.input_watermark,
        field_name=f"active_chat_{operation_name}.input_watermark",
    )
    _nonnegative_int(
        effect.input_ledger_sequence,
        field_name=f"active_chat_{operation_name}.input_ledger_sequence",
    )
    if not effect.target_session_id.startswith(f"{effect.instance_id}:"):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} effect has an invalid transport session"
        )
    if not isinstance(effect.ledger_entries, tuple):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} effect contains an invalid ledger snapshot"
        )
    return effect


def _select_bootstrap_handoff_entries(
    entries: object,
    *,
    effect: ActorWorkflowEffectInput,
    handoff_operation_id: str,
    handoff_message_log_ids: tuple[int, ...],
) -> tuple[MessageLedgerEntry, ...]:
    """Select and prove exactly the review-consumed bootstrap handoff rows."""

    ledger_entries = _validated_complete_ledger_entries(entries, key=effect.key)
    if not handoff_message_log_ids:
        return ()
    requested_ids = set(handoff_message_log_ids)
    selected_entries = tuple(
        entry for entry in ledger_entries if entry.message_log_id in requested_ids
    )
    selected_ids = {entry.message_log_id for entry in selected_entries}
    missing = [
        message_log_id
        for message_log_id in handoff_message_log_ids
        if message_log_id not in selected_ids
    ]
    if missing:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap handoff ledger entries disappeared: "
            + ", ".join(str(message_log_id) for message_log_id in missing)
        )
    if len(selected_entries) != len(handoff_message_log_ids):
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap handoff selection is not one-to-one"
        )
    for entry in selected_entries:
        _validate_effect_entry(
            entry,
            effect=effect,
            operation_name="bootstrap",
            require_unread=False,
        )
        provenance = entry.review_consumption
        if not isinstance(provenance, MessageConsumptionProvenance):
            raise ActorActiveChatWorkflowContextError(
                "actor active chat bootstrap handoff was not consumed by review: "
                + str(entry.message_log_id)
            )
        _validate_review_handoff_provenance(
            provenance,
            entry=entry,
            effect=effect,
            handoff_operation_id=handoff_operation_id,
        )
        if entry.chat_consumption is not None:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat bootstrap handoff was already consumed by chat: "
                + str(entry.message_log_id)
            )
    return selected_entries


def _validated_round_snapshot_entries(
    effect: ActorWorkflowEffectInput,
) -> tuple[MessageLedgerEntry, ...]:
    """Return the request's selected snapshot only after strict fence checks."""

    entries = _coerce_entry_sequence(
        effect.ledger_entries,
        field_name="actor active chat round effect ledger snapshot",
    )
    _validate_ledger_order(entries, key=effect.key, operation_name="round snapshot")
    for entry in entries:
        _validate_effect_entry(
            entry,
            effect=effect,
            operation_name="round snapshot",
            require_unread=True,
        )
    return entries


def _validated_round_captured_entries(
    entries: object,
    *,
    effect: ActorWorkflowEffectInput,
) -> tuple[MessageLedgerEntry, ...]:
    """Revalidate every row returned by the dual-fenced unread ledger port."""

    captured_entries = _coerce_entry_sequence(
        entries,
        field_name="actor active chat round captured unread ledger",
    )
    _validate_ledger_order(
        captured_entries,
        key=effect.key,
        operation_name="round captured unread",
    )
    for entry in captured_entries:
        _validate_effect_entry(
            entry,
            effect=effect,
            operation_name="round captured unread",
            require_unread=True,
        )
    return captured_entries


def _select_round_entries(
    captured_entries: tuple[MessageLedgerEntry, ...],
    *,
    effect: ActorWorkflowEffectInput,
    snapshot_entries: tuple[MessageLedgerEntry, ...],
    selected_message_log_ids: tuple[int, ...],
) -> tuple[MessageLedgerEntry, ...]:
    """Filter a captured unread projection to the exact selected IDs in order."""

    selected_ids = set(selected_message_log_ids)
    selected_entries = tuple(
        entry for entry in captured_entries if entry.message_log_id in selected_ids
    )
    selected_by_id = {entry.message_log_id: entry for entry in selected_entries}
    missing = [
        message_log_id
        for message_log_id in selected_message_log_ids
        if message_log_id not in selected_by_id
    ]
    if missing:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat round selected ledger entries disappeared: "
            + ", ".join(str(message_log_id) for message_log_id in missing)
        )
    if len(selected_entries) != len(selected_message_log_ids):
        raise ActorActiveChatWorkflowContextError(
            "actor active chat round selection is not one-to-one"
        )
    snapshot_by_id = {entry.message_log_id: entry for entry in snapshot_entries}
    for message_log_id in selected_message_log_ids:
        snapshot_entry = snapshot_by_id.get(message_log_id)
        captured_entry = selected_by_id[message_log_id]
        if snapshot_entry is None:
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round selection escaped its effect snapshot: "
                + str(message_log_id)
            )
        if not _same_ledger_message(snapshot_entry, captured_entry):
            raise ActorActiveChatWorkflowContextError(
                "actor active chat round captured ledger changed selected input: "
                + str(message_log_id)
            )
        _validate_effect_entry(
            captured_entry,
            effect=effect,
            operation_name="round selected",
            require_unread=True,
        )
    return selected_entries


def _validated_complete_ledger_entries(
    entries: object,
    *,
    key: SessionKey,
) -> tuple[MessageLedgerEntry, ...]:
    """Validate one full ledger response without filtering it by live history."""

    ledger_entries = _coerce_entry_sequence(
        entries,
        field_name="actor active chat bootstrap complete ledger",
    )
    _validate_ledger_order(
        ledger_entries,
        key=key,
        operation_name="bootstrap complete ledger",
    )
    return ledger_entries


def _coerce_entry_sequence(
    value: object,
    *,
    field_name: str,
) -> tuple[MessageLedgerEntry, ...]:
    """Return a non-string sequence of ledger entries without accepting iterators."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ActorActiveChatWorkflowContextError(f"{field_name} must be a sequence")
    return tuple(value)


def _validate_ledger_order(
    entries: tuple[MessageLedgerEntry, ...],
    *,
    key: SessionKey,
    operation_name: str,
) -> None:
    """Prove a ledger port returned one ordered, single-session projection."""

    previous_sequence = 0
    seen_message_log_ids: set[int] = set()
    for entry in entries:
        _validate_entry_shape(entry, key=key, operation_name=operation_name)
        if entry.ledger_sequence <= previous_sequence:
            raise ActorActiveChatWorkflowContextError(
                f"actor active chat {operation_name} ledger is not strictly ordered"
            )
        previous_sequence = entry.ledger_sequence
        if entry.message_log_id in seen_message_log_ids:
            raise ActorActiveChatWorkflowContextError(
                f"actor active chat {operation_name} ledger contains a duplicate message log id"
            )
        seen_message_log_ids.add(entry.message_log_id)


def _validate_entry_shape(
    entry: object,
    *,
    key: SessionKey,
    operation_name: str,
) -> None:
    """Validate durable row shape before accessing its actor-owned fields."""

    if not isinstance(entry, MessageLedgerEntry):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger contains an invalid entry"
        )
    if not isinstance(entry.message, AppendMessageLedgerEntry):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry has an invalid message"
        )
    if entry.key != key:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry belongs to another session"
        )
    _positive_int(
        entry.ledger_sequence,
        field_name=f"active_chat_{operation_name}.ledger_sequence",
    )
    _positive_int(
        entry.message_log_id,
        field_name=f"active_chat_{operation_name}.message_log_id",
    )
    for field_name in (
        "review_consumption",
        "chat_consumption",
        "high_priority_consumption",
    ):
        value = getattr(entry, field_name)
        if value is not None and not isinstance(value, MessageConsumptionProvenance):
            raise ActorActiveChatWorkflowContextError(
                f"actor active chat {operation_name} ledger entry has invalid {field_name}"
            )


def _validate_effect_entry(
    entry: MessageLedgerEntry,
    *,
    effect: ActorWorkflowEffectInput,
    operation_name: str,
    require_unread: bool,
) -> None:
    """Validate a model-visible entry against the effect's identity and fences."""

    _validate_entry_shape(entry, key=effect.key, operation_name=operation_name)
    if entry.message.ownership_generation != effect.ownership_generation:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry belongs to another ownership generation"
        )
    if entry.message_log_id > effect.input_watermark:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry exceeds the watermark fence"
        )
    if entry.ledger_sequence > effect.input_ledger_sequence:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry exceeds the sequence fence"
        )
    if entry.message.eligible_for_work is not True or entry.message.suppression_reason:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry is suppressed"
        )
    if entry.message.instance_id != effect.instance_id:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry belongs to another instance"
        )
    if entry.message.base_session_id != effect.target_session_id:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry has a different transport session"
        )
    if require_unread and not entry.is_unread:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} ledger entry is no longer unread"
        )


def _validate_review_handoff_provenance(
    provenance: MessageConsumptionProvenance,
    *,
    entry: MessageLedgerEntry,
    effect: ActorWorkflowEffectInput,
    handoff_operation_id: str,
) -> None:
    """Prove one handoff row was durably consumed by its named review operation."""

    if provenance.operation_id != handoff_operation_id:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap handoff has another review operation: "
            + str(entry.message_log_id)
        )
    if provenance.ownership_generation != effect.ownership_generation:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap handoff review consumption has another ownership "
            "generation: "
            + str(entry.message_log_id)
        )
    _required_text(
        provenance.consumption_id,
        field_name="active_chat_bootstrap.review_consumption.consumption_id",
    )
    _required_text(
        provenance.idempotency_key,
        field_name="active_chat_bootstrap.review_consumption.idempotency_key",
    )
    _required_text(
        provenance.source_event_id,
        field_name="active_chat_bootstrap.review_consumption.source_event_id",
    )
    _nonnegative_int(
        provenance.input_watermark,
        field_name="active_chat_bootstrap.review_consumption.input_watermark",
    )
    _nonnegative_int(
        provenance.input_ledger_sequence,
        field_name="active_chat_bootstrap.review_consumption.input_ledger_sequence",
    )
    if provenance.input_watermark < entry.message_log_id:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap review consumption does not cover message id: "
            + str(entry.message_log_id)
        )
    if provenance.input_ledger_sequence < entry.ledger_sequence:
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap review consumption does not cover ledger sequence: "
            + str(entry.message_log_id)
        )
    if not isinstance(provenance.committed_at, (int, float)) or isinstance(
        provenance.committed_at,
        bool,
    ) or not math.isfinite(float(provenance.committed_at)):
        raise ActorActiveChatWorkflowContextError(
            "actor active chat bootstrap review consumption has an invalid commit time"
        )


def _same_ledger_message(
    expected: MessageLedgerEntry,
    actual: MessageLedgerEntry,
) -> bool:
    """Return whether two unread selections identify the same immutable ledger row."""

    return (
        expected.message_log_id == actual.message_log_id
        and expected.ledger_sequence == actual.ledger_sequence
        and expected.message.ownership_generation == actual.message.ownership_generation
        and expected.message.canonical_json == actual.message.canonical_json
        and expected.review_consumption is None
        and expected.chat_consumption is None
        and actual.review_consumption is None
        and actual.chat_consumption is None
    )


def _validate_projected_stage_input(
    stage_input: object,
    *,
    effect: ActorWorkflowEffectInput,
    messages: list[MessageLogPayload],
    purpose: str,
    expected_metadata: dict[str, object],
    operation_name: str,
) -> None:
    """Reject builder output that adds hidden history or alters exact input."""

    if not isinstance(stage_input, ReviewStageInput):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder returned an invalid stage input"
        )
    if stage_input.session_id != effect.key.session_id:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder changed the actor session"
        )
    if stage_input.instance_id != effect.instance_id:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder changed the adapter instance"
        )
    if stage_input.purpose != purpose:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder changed the stage purpose"
        )
    if not isinstance(stage_input.source_messages, list) or len(
        stage_input.source_messages
    ) != len(messages):
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder changed source messages"
        )
    for expected, actual in zip(messages, stage_input.source_messages, strict=True):
        if not isinstance(actual, Mapping) or dict(actual) != expected:
            raise ActorActiveChatWorkflowContextError(
                f"actor active chat {operation_name} context builder changed source messages"
            )
    if stage_input.context_messages != [] or stage_input.instruction_content != []:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder added runtime history"
        )
    if not isinstance(stage_input.metadata, Mapping) or dict(stage_input.metadata) != expected_metadata:
        raise ActorActiveChatWorkflowContextError(
            f"actor active chat {operation_name} context builder changed stage metadata"
        )


def _required_text(value: object, *, field_name: str) -> str:
    """Return canonical non-empty text without silently normalizing input."""

    if not isinstance(value, str):
        raise ActorActiveChatWorkflowContextError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or normalized != value:
        raise ActorActiveChatWorkflowContextError(
            f"{field_name} must be a non-empty canonical string"
        )
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    """Return one positive integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ActorActiveChatWorkflowContextError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    """Return one non-negative integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ActorActiveChatWorkflowContextError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _positive_id_tuple(value: object, *, field_name: str) -> tuple[int, ...]:
    """Return an exact duplicate-free tuple of positive message-log IDs."""

    if not isinstance(value, tuple):
        raise ActorActiveChatWorkflowContextError(f"{field_name} must be a tuple")
    result: list[int] = []
    seen: set[int] = set()
    for index, item in enumerate(value):
        message_log_id = _positive_int(item, field_name=f"{field_name}[{index}]")
        if message_log_id in seen:
            raise ActorActiveChatWorkflowContextError(
                f"{field_name} contains a duplicate message log id: {message_log_id}"
            )
        seen.add(message_log_id)
        result.append(message_log_id)
    return tuple(result)


def _nonnegative_finite_number(value: object, *, field_name: str) -> float:
    """Return one finite non-negative numeric value without truthy coercion."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ActorActiveChatWorkflowContextError(f"{field_name} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0.0:
        raise ActorActiveChatWorkflowContextError(
            f"{field_name} must be a non-negative finite number"
        )
    return normalized


def _bootstrap_disposition(value: object, *, field_name: str) -> str:
    """Return the completed bootstrap disposition required by every round."""

    canonical = _required_text(value, field_name=field_name)
    try:
        return ActiveChatBootstrapDisposition(canonical).value
    except (TypeError, ValueError) as exc:
        raise ActorActiveChatWorkflowContextError(
            f"{field_name} is not a valid active chat bootstrap disposition"
        ) from exc


__all__ = [
    "ActorActiveChatBootstrapLedgerPort",
    "ActorActiveChatBootstrapStageInputProjector",
    "ActorActiveChatBootstrapWorkflowContextProjector",
    "ActorActiveChatRoundLedgerPort",
    "ActorActiveChatRoundStageInputProjector",
    "ActorActiveChatRoundWorkflowContextProjector",
    "ActorActiveChatWorkflowContextError",
    "ActorActiveChatWorkflowMessageStore",
]
