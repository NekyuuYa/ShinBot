"""Typed facts and mutations for the actor-owned message ledger.

The ledger stores one row per routed message.  Unread ranges are projections
over those rows rather than independently mutable durable state.  Consumption
is represented as an idempotent operation-scoped intent over rows present at
the operation's captured ledger boundary.  A later append never inherits an
earlier consumption from its numeric message id.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey


class _FrozenDict(dict[str, Any]):
    """JSON-compatible mapping that rejects in-place mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable message ledger mappings are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenList(list[Any]):
    """JSON-compatible sequence that rejects in-place mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable message ledger sequences are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | dict[str, JsonValue] | list[JsonValue]


class MessageLedgerConsumptionKind(StrEnum):
    """Independent consumption channels represented on each message fact."""

    REVIEW = "review"
    CHAT = "chat"
    HIGH_PRIORITY = "high_priority"


class MessageLedgerConsumptionSelection(StrEnum):
    """How an operation identifies messages inside its input watermark."""

    ALL_THROUGH_WATERMARK = "all_through_watermark"
    EXPLICIT_IDS = "explicit_ids"


class MessageLedgerProjectionKind(StrEnum):
    """Read-only views derived from the per-message ledger."""

    UNREAD = "unread"
    REVIEW_PENDING = "review_pending"
    CHAT_PENDING = "chat_pending"
    HIGH_PRIORITY_PENDING = "high_priority_pending"


class MessageWatermarkDisposition(StrEnum):
    """Relationship between a delivered message and captured operation input."""

    CAPTURED_OR_LATE = "captured_or_late"
    NEW_ACTIVITY = "new_activity"


@dataclass(slots=True, frozen=True)
class MessagePriorityFlags:
    """Priority facts calculated for one message by the actor policy."""

    mention: bool = False
    reply_to_bot: bool = False
    repeated_mention: bool = False
    poke_to_bot: bool = False
    should_wake_active_reply: bool = False
    reasons: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject truthy coercions and deeply freeze explanatory data."""

        for field_name in (
            "mention",
            "reply_to_bot",
            "repeated_mention",
            "poke_to_bot",
            "should_wake_active_reply",
        ):
            _require_bool(getattr(self, field_name), field_name=field_name)
        object.__setattr__(
            self,
            "reasons",
            _freeze_json_object(self.reasons, field_name="priority reasons"),
        )
        if self.should_wake_active_reply and not self.is_high_priority:
            raise ValueError(
                "should_wake_active_reply requires at least one priority flag"
            )

    @property
    def is_high_priority(self) -> bool:
        """Return whether any durable high-priority condition applies."""

        return any(
            (
                self.mention,
                self.reply_to_bot,
                self.repeated_mention,
                self.poke_to_bot,
            )
        )

    def to_record(self) -> dict[str, object]:
        """Return a plain JSON-compatible persistence mapping."""

        return {
            "mention": self.mention,
            "reply_to_bot": self.reply_to_bot,
            "repeated_mention": self.repeated_mention,
            "poke_to_bot": self.poke_to_bot,
            "should_wake_active_reply": self.should_wake_active_reply,
            "reasons": _thaw_json(self.reasons),
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class AppendMessageLedgerEntry:
    """Canonical message fact appended by a ``MessageReceived`` transition.

    ``actor_event_id`` is copied from the versioned route payload while
    ``source_event_id`` is the claimed mailbox identity.  Requiring equality
    prevents a valid message payload from being committed under another event.
    ``recorded_at`` is deliberately absent: the store assigns it from the
    transaction commit clock after taking the SQLite write lock.
    """

    key: SessionKey
    message_log_id: int
    ownership_generation: int
    source_event_id: str
    actor_event_id: str
    delivery_version: int
    event_source: str
    sender_id: str
    instance_id: str
    event_type: str
    bot_id: str = ""
    bot_binding_id: str = ""
    base_session_id: str = ""
    bot_session_id: str = ""
    platform: str = ""
    self_id: str = ""
    is_private: bool = False
    is_mentioned: bool = False
    is_mention_to_other: bool = False
    is_reply_to_bot: bool = False
    is_poke_to_bot: bool = False
    is_poke_to_other: bool = False
    already_handled: bool = False
    is_stopped: bool = False
    is_self_message: bool = False
    eligible_for_work: bool = True
    suppression_reason: str = ""
    response_profile: str = ""
    priority: MessagePriorityFlags = field(default_factory=MessagePriorityFlags)
    causation_id: str = ""
    correlation_id: str = ""
    trace_id: str = ""
    observed_at: float = 0.0
    occurred_at: float = 0.0
    event_created_at: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize canonical text and reject ambiguous durable values."""

        _require_positive_int(self.message_log_id, field_name="message_log_id")
        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        _require_positive_int(self.delivery_version, field_name="delivery_version")
        required_text = (
            "source_event_id",
            "actor_event_id",
            "event_source",
            "instance_id",
            "event_type",
        )
        text_fields = (
            *required_text,
            "sender_id",
            "bot_id",
            "bot_binding_id",
            "base_session_id",
            "bot_session_id",
            "platform",
            "self_id",
            "response_profile",
            "causation_id",
            "correlation_id",
            "trace_id",
        )
        for field_name in text_fields:
            normalized = _normalize_text(getattr(self, field_name))
            if field_name in required_text and not normalized:
                raise ValueError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, normalized)
        if self.source_event_id != self.actor_event_id:
            raise ValueError(
                "actor_event_id must match the claimed source_event_id"
            )
        for field_name in (
            "is_private",
            "is_mentioned",
            "is_mention_to_other",
            "is_reply_to_bot",
            "is_poke_to_bot",
            "is_poke_to_other",
            "already_handled",
            "is_stopped",
            "is_self_message",
            "eligible_for_work",
        ):
            _require_bool(getattr(self, field_name), field_name=field_name)
        suppression_reason = _normalize_text(self.suppression_reason)
        if self.eligible_for_work and suppression_reason:
            raise ValueError("eligible messages cannot carry a suppression reason")
        if not self.eligible_for_work and not suppression_reason:
            raise ValueError("ineligible messages require a suppression reason")
        object.__setattr__(self, "suppression_reason", suppression_reason)
        if not isinstance(self.priority, MessagePriorityFlags):
            raise TypeError("priority must be MessagePriorityFlags")
        for field_name in ("observed_at", "occurred_at", "event_created_at"):
            object.__setattr__(
                self,
                field_name,
                _nonnegative_finite(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "metadata",
            _freeze_json_object(self.metadata, field_name="message metadata"),
        )

    def to_record(self) -> dict[str, object]:
        """Return the canonical content used for persistence and replay checks."""

        return {
            "profile_id": self.key.profile_id,
            "session_id": self.key.session_id,
            "message_log_id": self.message_log_id,
            "ownership_generation": self.ownership_generation,
            "source_event_id": self.source_event_id,
            "actor_event_id": self.actor_event_id,
            "delivery_version": self.delivery_version,
            "event_source": self.event_source,
            "sender_id": self.sender_id,
            "instance_id": self.instance_id,
            "event_type": self.event_type,
            "bot_id": self.bot_id,
            "bot_binding_id": self.bot_binding_id,
            "base_session_id": self.base_session_id,
            "bot_session_id": self.bot_session_id,
            "platform": self.platform,
            "self_id": self.self_id,
            "is_private": self.is_private,
            "is_mentioned": self.is_mentioned,
            "is_mention_to_other": self.is_mention_to_other,
            "is_reply_to_bot": self.is_reply_to_bot,
            "is_poke_to_bot": self.is_poke_to_bot,
            "is_poke_to_other": self.is_poke_to_other,
            "already_handled": self.already_handled,
            "is_stopped": self.is_stopped,
            "is_self_message": self.is_self_message,
            "eligible_for_work": self.eligible_for_work,
            "suppression_reason": self.suppression_reason,
            "response_profile": self.response_profile,
            "priority": self.priority.to_record(),
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "trace_id": self.trace_id,
            "observed_at": self.observed_at,
            "occurred_at": self.occurred_at,
            "event_created_at": self.event_created_at,
            "metadata": _thaw_json(self.metadata),
        }

    @property
    def canonical_json(self) -> str:
        """Return stable business identity, excluding the mutable owner fence."""

        record = self.to_record()
        record.pop("ownership_generation")
        return _canonical_json(record)


@dataclass(slots=True, frozen=True, kw_only=True)
class ConsumeMessageLedgerEntries:
    """Operation-scoped consumption fenced by message and ledger boundaries.

    Selection is explicit: ``all_through_watermark`` covers every applicable
    row through the captured snapshot, while ``explicit_ids`` covers exactly
    the supplied non-empty set.  Most workflow completions should use explicit
    IDs.  Merely classifying a delayed message as old activity does not prove a
    workflow consumed it.
    """

    key: SessionKey
    kind: MessageLedgerConsumptionKind
    consumption_id: str
    idempotency_key: str
    operation_id: str
    source_event_id: str
    ownership_generation: int
    input_watermark: int
    input_ledger_sequence: int
    selection: MessageLedgerConsumptionSelection
    explicit_message_log_ids: tuple[int, ...] = ()
    reason: str = ""
    trace_id: str = ""
    occurred_at: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Canonicalize selection identity and validate operation fencing."""

        try:
            kind = MessageLedgerConsumptionKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unsupported message consumption kind: {self.kind}") from exc
        object.__setattr__(self, "kind", kind)
        try:
            selection = MessageLedgerConsumptionSelection(self.selection)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"unsupported message consumption selection: {self.selection}"
            ) from exc
        object.__setattr__(self, "selection", selection)
        for field_name in (
            "consumption_id",
            "idempotency_key",
            "operation_id",
            "source_event_id",
        ):
            normalized = _normalize_text(getattr(self, field_name))
            if not normalized:
                raise ValueError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, normalized)
        object.__setattr__(self, "reason", _normalize_text(self.reason))
        object.__setattr__(self, "trace_id", _normalize_text(self.trace_id))
        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        _require_nonnegative_int(self.input_watermark, field_name="input_watermark")
        _require_nonnegative_int(
            self.input_ledger_sequence,
            field_name="input_ledger_sequence",
        )
        raw_ids = self.explicit_message_log_ids
        if not isinstance(raw_ids, (list, tuple)):
            raise TypeError("explicit_message_log_ids must be a list or tuple")
        normalized_ids: list[int] = []
        seen: set[int] = set()
        for value in raw_ids:
            message_log_id = _require_positive_int(
                value,
                field_name="explicit_message_log_ids item",
            )
            if message_log_id > self.input_watermark:
                raise ValueError(
                    "explicit message ids cannot exceed the input watermark"
                )
            if message_log_id in seen:
                raise ValueError("explicit_message_log_ids must not contain duplicates")
            seen.add(message_log_id)
            normalized_ids.append(message_log_id)
        object.__setattr__(
            self,
            "explicit_message_log_ids",
            tuple(sorted(normalized_ids)),
        )
        if (
            selection is MessageLedgerConsumptionSelection.EXPLICIT_IDS
            and not normalized_ids
        ):
            raise ValueError("explicit_ids selection requires at least one message id")
        if (
            selection is MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK
            and normalized_ids
        ):
            raise ValueError(
                "all_through_watermark selection cannot carry explicit message ids"
            )
        object.__setattr__(
            self,
            "occurred_at",
            _nonnegative_finite(self.occurred_at, field_name="occurred_at"),
        )
        object.__setattr__(
            self,
            "metadata",
            _freeze_json_object(self.metadata, field_name="consumption metadata"),
        )

    def covers(self, message_log_id: int, *, ledger_sequence: int) -> bool:
        """Return whether both operation input fences select one ledger row."""

        normalized_id = _require_positive_int(
            message_log_id,
            field_name="message_log_id",
        )
        normalized_sequence = _require_positive_int(
            ledger_sequence,
            field_name="ledger_sequence",
        )
        if (
            normalized_id > self.input_watermark
            or normalized_sequence > self.input_ledger_sequence
        ):
            return False
        return (
            self.selection
            is MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK
            or normalized_id in self.explicit_message_log_ids
        )

    def to_record(self) -> dict[str, object]:
        """Return canonical content used by idempotent persistence checks."""

        return {
            "profile_id": self.key.profile_id,
            "session_id": self.key.session_id,
            "kind": self.kind.value,
            "consumption_id": self.consumption_id,
            "idempotency_key": self.idempotency_key,
            "operation_id": self.operation_id,
            "source_event_id": self.source_event_id,
            "ownership_generation": self.ownership_generation,
            "input_watermark": self.input_watermark,
            "input_ledger_sequence": self.input_ledger_sequence,
            "selection": self.selection.value,
            "explicit_message_log_ids": list(self.explicit_message_log_ids),
            "reason": self.reason,
            "trace_id": self.trace_id,
            "occurred_at": self.occurred_at,
            "metadata": _thaw_json(self.metadata),
        }

    @property
    def canonical_json(self) -> str:
        """Return stable intent identity, excluding the mutable owner fence."""

        record = self.to_record()
        record.pop("ownership_generation")
        return _canonical_json(record)


type MessageLedgerMutation = AppendMessageLedgerEntry | ConsumeMessageLedgerEntries


@dataclass(slots=True, frozen=True, kw_only=True)
class MessageConsumptionProvenance:
    """The durable operation and event that consumed one ledger message."""

    consumption_id: str
    idempotency_key: str
    operation_id: str
    source_event_id: str
    input_watermark: int
    input_ledger_sequence: int
    ownership_generation: int
    committed_at: float

    def __post_init__(self) -> None:
        """Validate persisted provenance loaded by projections."""

        for field_name in (
            "consumption_id",
            "idempotency_key",
            "operation_id",
            "source_event_id",
        ):
            normalized = _normalize_text(getattr(self, field_name))
            if not normalized:
                raise ValueError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, normalized)
        _require_nonnegative_int(self.input_watermark, field_name="input_watermark")
        _require_nonnegative_int(
            self.input_ledger_sequence,
            field_name="input_ledger_sequence",
        )
        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        object.__setattr__(
            self,
            "committed_at",
            _nonnegative_finite(self.committed_at, field_name="committed_at"),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class MessageLedgerEntry:
    """One stored message fact with independent consumption provenance."""

    message: AppendMessageLedgerEntry
    ledger_sequence: int
    recorded_at: float
    updated_at: float
    review_consumption: MessageConsumptionProvenance | None = None
    chat_consumption: MessageConsumptionProvenance | None = None
    high_priority_consumption: MessageConsumptionProvenance | None = None

    def __post_init__(self) -> None:
        """Validate commit-clock timestamps and provenance types."""

        if not isinstance(self.message, AppendMessageLedgerEntry):
            raise TypeError("message must be AppendMessageLedgerEntry")
        _require_positive_int(self.ledger_sequence, field_name="ledger_sequence")
        recorded_at = _nonnegative_finite(
            self.recorded_at,
            field_name="recorded_at",
        )
        updated_at = _nonnegative_finite(self.updated_at, field_name="updated_at")
        if updated_at < recorded_at:
            raise ValueError("updated_at cannot precede recorded_at")
        object.__setattr__(self, "recorded_at", recorded_at)
        object.__setattr__(self, "updated_at", updated_at)
        for field_name in (
            "review_consumption",
            "chat_consumption",
            "high_priority_consumption",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(
                value,
                MessageConsumptionProvenance,
            ):
                raise TypeError(
                    f"{field_name} must be MessageConsumptionProvenance or None"
                )

    @property
    def key(self) -> SessionKey:
        """Return the profile-scoped actor key."""

        return self.message.key

    @property
    def message_log_id(self) -> int:
        """Return the source message-log identity."""

        return self.message.message_log_id

    @property
    def is_unread(self) -> bool:
        """Return whether neither review nor active chat consumed this row."""

        return (
            self.message.eligible_for_work
            and self.review_consumption is None
            and self.chat_consumption is None
        )

    @property
    def is_high_priority_pending(self) -> bool:
        """Return whether high-priority work remains for this message."""

        return (
            self.message.priority.is_high_priority
            and self.message.eligible_for_work
            and self.high_priority_consumption is None
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class MessageLedgerRangeProjection:
    """A contiguous projection run in durable ledger sequence order.

    ``start_message_log_id`` and ``end_message_log_id`` identify the first and
    last rows in ledger order; they are not numeric minima/maxima.  Persisted
    message-log IDs and source timestamps may arrive out of order.
    """

    key: SessionKey
    start_ledger_sequence: int
    end_ledger_sequence: int
    start_message_log_id: int
    end_message_log_id: int
    start_at: float
    end_at: float
    message_count: int

    def __post_init__(self) -> None:
        """Validate range ordering and count."""

        _require_positive_int(
            self.start_ledger_sequence,
            field_name="start_ledger_sequence",
        )
        _require_positive_int(
            self.end_ledger_sequence,
            field_name="end_ledger_sequence",
        )
        if self.end_ledger_sequence < self.start_ledger_sequence:
            raise ValueError("message ledger sequence range cannot move backwards")
        _require_positive_int(
            self.start_message_log_id,
            field_name="start_message_log_id",
        )
        _require_positive_int(
            self.end_message_log_id,
            field_name="end_message_log_id",
        )
        _require_positive_int(self.message_count, field_name="message_count")
        start_at = _nonnegative_finite(self.start_at, field_name="start_at")
        end_at = _nonnegative_finite(self.end_at, field_name="end_at")
        if end_at < start_at:
            raise ValueError("message ledger range time cannot move backwards")
        object.__setattr__(self, "start_at", start_at)
        object.__setattr__(self, "end_at", end_at)


def classify_message_watermark(
    message_log_id: int,
    *,
    input_watermark: int,
) -> MessageWatermarkDisposition:
    """Classify delayed old input separately from genuinely new activity."""

    normalized_id = _require_positive_int(message_log_id, field_name="message_log_id")
    normalized_watermark = _require_nonnegative_int(
        input_watermark,
        field_name="input_watermark",
    )
    if normalized_id <= normalized_watermark:
        return MessageWatermarkDisposition.CAPTURED_OR_LATE
    return MessageWatermarkDisposition.NEW_ACTIVITY


def append_message_ledger_entry_from_payload(
    payload: Mapping[str, object],
    *,
    key: SessionKey,
    ownership_generation: int,
    source_event_id: str,
    event_source: str,
    occurred_at: float,
    event_created_at: float,
    causation_id: str,
    correlation_id: str,
    trace_id: str,
    response_profile: str = "",
    priority: MessagePriorityFlags | None = None,
) -> AppendMessageLedgerEntry:
    """Build one append mutation from the rule-independent mailbox contract.

    The actor layer intentionally does not import the core route-delivery
    class.  It revalidates the versioned primitive payload so persisted data
    cannot redirect a claimed mailbox event to another session or message.
    """

    if not isinstance(payload, Mapping):
        raise TypeError("MessageReceived payload must be an object")
    raw_key = payload.get("session_key")
    if not isinstance(raw_key, Mapping):
        raise ValueError("MessageReceived session_key must be an object")
    payload_key = SessionKey(
        profile_id=_payload_required_text(raw_key, "profile_id"),
        session_id=_payload_required_text(raw_key, "session_id"),
    )
    if payload_key != key:
        raise ValueError("MessageReceived session_key does not match actor key")
    payload_trace_id = _payload_optional_text(payload, "trace_id")
    normalized_trace_id = _normalize_text(trace_id)
    if payload_trace_id != normalized_trace_id:
        raise ValueError("MessageReceived trace_id does not match mailbox envelope")
    is_mentioned = _payload_required_bool(payload, "is_mentioned")
    is_reply_to_bot = _payload_required_bool(payload, "is_reply_to_bot")
    is_poke_to_bot = _payload_required_bool(payload, "is_poke_to_bot")
    resolved_priority = priority or MessagePriorityFlags(
        mention=is_mentioned,
        reply_to_bot=is_reply_to_bot,
        poke_to_bot=is_poke_to_bot,
        should_wake_active_reply=any(
            (is_mentioned, is_reply_to_bot, is_poke_to_bot)
        ),
        reasons={
            name: reason
            for name, enabled, reason in (
                ("mention", is_mentioned, "message_mentions_self"),
                ("reply_to_bot", is_reply_to_bot, "message_replies_to_self"),
                ("poke_to_bot", is_poke_to_bot, "message_pokes_self"),
            )
            if enabled
        },
    )
    sender_id = _payload_optional_text(payload, "sender_id")
    self_id = _payload_optional_text(payload, "self_id")
    already_handled = _payload_required_bool(payload, "already_handled")
    is_stopped = _payload_required_bool(payload, "is_stopped")
    is_self_message = bool(sender_id and self_id and sender_id == self_id)
    suppression_reason = ""
    if already_handled:
        suppression_reason = "already_handled"
    elif is_stopped:
        suppression_reason = "stopped"
    elif is_self_message:
        suppression_reason = "self_message"
    return AppendMessageLedgerEntry(
        key=key,
        message_log_id=_payload_required_positive_int(payload, "message_log_id"),
        ownership_generation=ownership_generation,
        source_event_id=source_event_id,
        actor_event_id=_payload_required_text(payload, "event_id"),
        delivery_version=_payload_required_positive_int(payload, "version"),
        event_source=event_source,
        sender_id=sender_id,
        instance_id=_payload_required_text(payload, "instance_id"),
        event_type=_payload_required_text(payload, "event_type"),
        bot_id=_payload_optional_text(payload, "bot_id"),
        bot_binding_id=_payload_optional_text(payload, "bot_binding_id"),
        base_session_id=_payload_required_text(payload, "base_session_id"),
        bot_session_id=_payload_optional_text(payload, "bot_session_id"),
        platform=_payload_optional_text(payload, "platform"),
        self_id=self_id,
        is_private=_payload_required_bool(payload, "is_private"),
        is_mentioned=is_mentioned,
        is_mention_to_other=_payload_required_bool(payload, "is_mention_to_other"),
        is_reply_to_bot=is_reply_to_bot,
        is_poke_to_bot=is_poke_to_bot,
        is_poke_to_other=_payload_required_bool(payload, "is_poke_to_other"),
        already_handled=already_handled,
        is_stopped=is_stopped,
        is_self_message=is_self_message,
        eligible_for_work=not suppression_reason,
        suppression_reason=suppression_reason,
        response_profile=response_profile,
        priority=resolved_priority,
        causation_id=causation_id,
        correlation_id=correlation_id,
        trace_id=normalized_trace_id,
        observed_at=_payload_required_nonnegative_float(payload, "observed_at"),
        occurred_at=occurred_at,
        event_created_at=event_created_at,
    )


def validate_message_ledger_mutations(
    mutations: tuple[MessageLedgerMutation, ...],
    *,
    key: SessionKey,
    ownership_generation: int,
    source_event_id: str,
) -> tuple[MessageLedgerMutation, ...]:
    """Fence a transition's complete ledger mutation batch before SQL writes."""

    if not isinstance(mutations, tuple):
        raise TypeError("message ledger mutations must be a tuple")
    normalized_generation = _require_positive_int(
        ownership_generation,
        field_name="ownership_generation",
    )
    normalized_source_event_id = _normalize_text(source_event_id)
    if not normalized_source_event_id:
        raise ValueError("source_event_id must not be empty")
    append_count = 0
    identities: set[tuple[str, str]] = set()
    for mutation in mutations:
        if not isinstance(
            mutation,
            (AppendMessageLedgerEntry, ConsumeMessageLedgerEntries),
        ):
            raise TypeError("unsupported message ledger mutation")
        if mutation.key != key:
            raise ValueError("message ledger mutation key does not match actor key")
        if mutation.ownership_generation != normalized_generation:
            raise ValueError(
                "message ledger mutation ownership generation does not match claim"
            )
        if mutation.source_event_id != normalized_source_event_id:
            raise ValueError(
                "message ledger mutation source event does not match mailbox claim"
            )
        if isinstance(mutation, AppendMessageLedgerEntry):
            append_count += 1
            identity = ("append", str(mutation.message_log_id))
        else:
            identity = ("consumption", mutation.consumption_id)
        if identity in identities:
            raise ValueError("duplicate message ledger mutation identity in transition")
        identities.add(identity)
    if append_count > 1:
        raise ValueError("one transition cannot append more than one message")
    return mutations


def project_message_ranges(
    entries: list[MessageLedgerEntry] | tuple[MessageLedgerEntry, ...],
    *,
    kind: MessageLedgerProjectionKind = MessageLedgerProjectionKind.UNREAD,
) -> tuple[MessageLedgerRangeProjection, ...]:
    """Derive ordered range runs without persisting a second range model.

    The input must contain the complete ordered ledger slice, including rows
    outside the requested projection.  Such rows split a range.  Message-log
    IDs need not be numerically adjacent because IDs are global and other
    sessions may occupy the gaps.
    """

    try:
        projection_kind = MessageLedgerProjectionKind(kind)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unsupported message projection kind: {kind}") from exc
    if not isinstance(entries, (list, tuple)):
        raise TypeError("entries must be a list or tuple")
    if not entries:
        return ()
    ordered = sorted(entries, key=lambda item: item.ledger_sequence)
    key = ordered[0].key
    seen_ids: set[int] = set()
    seen_sequences: set[int] = set()
    runs: list[list[MessageLedgerEntry]] = []
    current: list[MessageLedgerEntry] = []
    for entry in ordered:
        if not isinstance(entry, MessageLedgerEntry):
            raise TypeError("entries must contain MessageLedgerEntry values")
        if entry.key != key:
            raise ValueError("one range projection cannot mix session keys")
        if entry.message_log_id in seen_ids:
            raise ValueError("range projection cannot contain duplicate message ids")
        if entry.ledger_sequence in seen_sequences:
            raise ValueError("range projection cannot contain duplicate ledger sequences")
        seen_ids.add(entry.message_log_id)
        seen_sequences.add(entry.ledger_sequence)
        if _matches_projection(entry, projection_kind):
            current.append(entry)
            continue
        if current:
            runs.append(current)
            current = []
    if current:
        runs.append(current)
    return tuple(
        MessageLedgerRangeProjection(
            key=key,
            start_ledger_sequence=run[0].ledger_sequence,
            end_ledger_sequence=run[-1].ledger_sequence,
            start_message_log_id=run[0].message_log_id,
            end_message_log_id=run[-1].message_log_id,
            start_at=min(item.message.occurred_at for item in run),
            end_at=max(item.message.occurred_at for item in run),
            message_count=len(run),
        )
        for run in runs
    )


def count_projected_messages(
    entries: list[MessageLedgerEntry] | tuple[MessageLedgerEntry, ...],
    *,
    kind: MessageLedgerProjectionKind = MessageLedgerProjectionKind.UNREAD,
) -> int:
    """Count rows in one derived ledger projection."""

    try:
        projection_kind = MessageLedgerProjectionKind(kind)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unsupported message projection kind: {kind}") from exc
    return sum(1 for entry in entries if _matches_projection(entry, projection_kind))


def _matches_projection(
    entry: MessageLedgerEntry,
    kind: MessageLedgerProjectionKind,
) -> bool:
    if not isinstance(entry, MessageLedgerEntry):
        raise TypeError("entries must contain MessageLedgerEntry values")
    if kind is MessageLedgerProjectionKind.UNREAD:
        return entry.is_unread
    if kind in {
        MessageLedgerProjectionKind.REVIEW_PENDING,
        MessageLedgerProjectionKind.CHAT_PENDING,
    }:
        # A message is handed to at most one conversational workflow.  The
        # separate columns preserve provenance; they are not independent work
        # queues that may expose the same message twice.
        return entry.is_unread
    return entry.is_high_priority_pending


def _normalize_text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("durable message ledger text fields must be strings")
    return value.strip()


def _payload_optional_text(values: Mapping[str, object], field_name: str) -> str:
    value = values.get(field_name, "")
    if not isinstance(value, str):
        raise TypeError(f"MessageReceived {field_name} must be a string")
    return value.strip()


def _payload_required_text(values: Mapping[str, object], field_name: str) -> str:
    value = _payload_optional_text(values, field_name)
    if not value:
        raise ValueError(f"MessageReceived {field_name} must not be empty")
    return value


def _payload_required_positive_int(
    values: Mapping[str, object],
    field_name: str,
) -> int:
    return _require_positive_int(values.get(field_name), field_name=field_name)


def _payload_required_bool(values: Mapping[str, object], field_name: str) -> bool:
    return _require_bool(values.get(field_name), field_name=field_name)


def _payload_required_nonnegative_float(
    values: Mapping[str, object],
    field_name: str,
) -> float:
    return _nonnegative_finite(values.get(field_name), field_name=field_name)


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a boolean")
    return value


def _require_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _require_nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _freeze_json_object(value: object, *, field_name: str) -> _FrozenDict:
    frozen = _freeze_json(value, path=field_name)
    if not isinstance(frozen, _FrozenDict):
        raise TypeError(f"{field_name} must be an object")
    return frozen


def _freeze_json(value: object, *, path: str) -> JsonValue:
    if isinstance(value, dict):
        items: list[tuple[str, JsonValue]] = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings")
            items.append((key, _freeze_json(item, path=f"{path}.{key}")))
        return _FrozenDict(items)
    if isinstance(value, (list, tuple)):
        return _FrozenList(
            _freeze_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} numbers must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"{path} values must be JSON-compatible, got {type(value)!r}")


def _thaw_json(value: object) -> JsonValue:
    if isinstance(value, dict):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unexpected frozen JSON value: {type(value)!r}")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = [
    "AppendMessageLedgerEntry",
    "ConsumeMessageLedgerEntries",
    "MessageConsumptionProvenance",
    "MessageLedgerConsumptionKind",
    "MessageLedgerConsumptionSelection",
    "MessageLedgerEntry",
    "MessageLedgerMutation",
    "MessageLedgerProjectionKind",
    "MessageLedgerRangeProjection",
    "MessagePriorityFlags",
    "MessageWatermarkDisposition",
    "append_message_ledger_entry_from_payload",
    "classify_message_watermark",
    "count_projected_messages",
    "project_message_ranges",
    "validate_message_ledger_mutations",
]
