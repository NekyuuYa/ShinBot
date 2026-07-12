"""Versioned wire contracts for session-actor workflow completions.

Workflow handlers run outside the actor transaction.  They may return model
decisions and normalized action proposals, but never actor-owned identities,
absolute schedule timestamps, or executable effects.  This module is the
strict JSON boundary between those handlers and completion mailbox events.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar

from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)

EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION = 1
WORKFLOW_COMPLETION_SCHEMA_VERSION = 1

_RUNTIME_RESERVED_FIELDS = frozenset(
    {
        "active_epoch",
        "activity_generation",
        "claim_id",
        "contract_signature",
        "contract_version",
        "effect_id",
        "expected_active_epoch",
        "expected_activity_generation",
        "idempotency_key",
        "input_ledger_sequence",
        "input_watermark",
        "instance_id",
        "operation_id",
        "ownership_generation",
        "profile_id",
        "session_id",
        "source_event_id",
        "state_revision",
        "target_session_id",
    }
)
_INTENT_BATCH_FIELDS = frozenset({"schema_version", "intents"})
_INTENT_FIELDS = frozenset({"proposal_id", "action_ordinal", "kind", "payload"})
_ACTIVE_REPLY_FIELDS = frozenset(
    {
        "schema_version",
        "completion_type",
        "consumed_message_log_ids",
        "external_actions",
    }
)
_REVIEW_FIELDS = frozenset(
    {
        "schema_version",
        "completion_type",
        "consumed_message_log_ids",
        "external_actions",
        "enter_active_chat",
        "next_review_outcome",
    }
)
_ACTIVE_CHAT_BOOTSTRAP_FIELDS = frozenset(
    {
        "schema_version",
        "completion_type",
        "disposition",
        "reason",
    }
)
_ACTIVE_CHAT_ROUND_FIELDS = frozenset(
    {
        "schema_version",
        "completion_type",
        "consumed_message_log_ids",
        "external_actions",
        "outcome",
        "interest_delta",
        "reason",
    }
)
_NEXT_REVIEW_FIELDS = frozenset(
    {
        "kind",
        "applied_delay_seconds",
        "requested_delay_seconds",
        "reason",
        "fallback_reason",
    }
)


class WorkflowCompletionCodecError(ValueError):
    """Raised when workflow completion wire data is ambiguous or unsupported."""


class ReviewNextReviewOutcomeKind(StrEnum):
    """Typed relative schedule outcomes accepted from a review workflow."""

    PLANNED = "planned"
    DEFAULTED = "defaulted"
    FAILED = "failed"
    BYPASSED = "bypassed"


class ActiveChatBootstrapDisposition(StrEnum):
    """Model-visible bootstrap curve choices accepted by the actor."""

    EXIT_SOON = "exit_soon"
    WATCH = "watch"
    CASUAL = "casual"
    ENGAGED = "engaged"
    FOCUSED = "focused"


class ActiveChatRoundOutcome(StrEnum):
    """Terminal outcome of one active-chat round workflow."""

    CONTINUE = "continue"
    EXIT = "exit"
    RETRY = "retry"


@dataclass(slots=True, frozen=True)
class ExternalActionIntentBatch:
    """One operation-global, ordered batch of external action proposals."""

    intents: tuple[ExternalActionIntent, ...] = ()

    schema_version: ClassVar[int] = EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Freeze and validate proposal identity and operation-global ordering."""

        intents = tuple(self.intents)
        _validate_external_action_intents(intents)
        object.__setattr__(self, "intents", intents)

    def to_payload(self) -> dict[str, Any]:
        """Encode the batch as strict JSON-compatible versioned data."""

        return {
            "schema_version": self.schema_version,
            "intents": [
                {
                    "proposal_id": intent.tool_call_id,
                    "action_ordinal": intent.action_ordinal,
                    "kind": intent.kind.value,
                    "payload": _plain_json_object(
                        intent.payload,
                        field_name=f"intents[{index}].payload",
                    ),
                }
                for index, intent in enumerate(self.intents)
            ],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ExternalActionIntentBatch:
        """Decode one strict version of an external action intent batch."""

        values = _strict_object(payload, _INTENT_BATCH_FIELDS, field_name="external_actions")
        _require_schema_version(
            values.get("schema_version"),
            expected=cls.schema_version,
            field_name="external_actions.schema_version",
        )
        raw_intents = values.get("intents")
        if not isinstance(raw_intents, list):
            raise WorkflowCompletionCodecError("external_actions.intents must be an array")
        intents = tuple(
            _decode_external_action_intent(item, index=index)
            for index, item in enumerate(raw_intents)
        )
        return cls(intents=intents)

    def to_json(self) -> str:
        """Encode the batch as canonical JSON."""

        return _canonical_json(self.to_payload())

    @classmethod
    def from_json(cls, payload: str) -> ExternalActionIntentBatch:
        """Decode a batch from a strict JSON object."""

        return cls.from_payload(_json_object(payload, field_name="external_actions"))


@dataclass(slots=True, frozen=True)
class ReviewNextReviewOutcome:
    """Relative next-review schedule decision carried by a review completion."""

    kind: ReviewNextReviewOutcomeKind
    applied_delay_seconds: float
    reason: str
    requested_delay_seconds: float | None = None
    fallback_reason: str = ""

    def __post_init__(self) -> None:
        """Validate a relative, reducer-committable schedule outcome."""

        try:
            kind = ReviewNextReviewOutcomeKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise WorkflowCompletionCodecError(
                f"unsupported next review outcome kind: {self.kind!r}"
            ) from exc
        applied = _nonnegative_finite(
            self.applied_delay_seconds,
            field_name="next_review_outcome.applied_delay_seconds",
        )
        requested = self.requested_delay_seconds
        if requested is not None:
            requested = _nonnegative_finite(
                requested,
                field_name="next_review_outcome.requested_delay_seconds",
            )
        if kind is ReviewNextReviewOutcomeKind.PLANNED and requested is None:
            raise WorkflowCompletionCodecError(
                "a planned next review outcome requires requested_delay_seconds"
            )
        reason = _required_text(self.reason, field_name="next_review_outcome.reason")
        fallback_reason = _optional_text(
            self.fallback_reason,
            field_name="next_review_outcome.fallback_reason",
        )
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "applied_delay_seconds", applied)
        object.__setattr__(self, "requested_delay_seconds", requested)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "fallback_reason", fallback_reason)

    def to_payload(self) -> dict[str, Any]:
        """Encode this typed outcome without assigning an absolute deadline."""

        return {
            "kind": self.kind.value,
            "applied_delay_seconds": self.applied_delay_seconds,
            "requested_delay_seconds": self.requested_delay_seconds,
            "reason": self.reason,
            "fallback_reason": self.fallback_reason,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ReviewNextReviewOutcome:
        """Decode a strict typed next-review outcome."""

        values = _strict_object(
            payload,
            _NEXT_REVIEW_FIELDS,
            field_name="next_review_outcome",
        )
        return cls(
            kind=_review_outcome_kind(values.get("kind")),
            applied_delay_seconds=_number(
                values.get("applied_delay_seconds"),
                field_name="next_review_outcome.applied_delay_seconds",
            ),
            requested_delay_seconds=_optional_number(
                values.get("requested_delay_seconds"),
                field_name="next_review_outcome.requested_delay_seconds",
            ),
            reason=_required_text(
                values.get("reason"),
                field_name="next_review_outcome.reason",
            ),
            fallback_reason=_optional_text(
                values.get("fallback_reason"),
                field_name="next_review_outcome.fallback_reason",
            ),
        )


@dataclass(slots=True, frozen=True)
class ActiveReplyCompletionResult:
    """Typed durable result returned by an active-reply workflow effect."""

    consumed_message_log_ids: tuple[int, ...] = ()
    external_action_intents: tuple[ExternalActionIntent, ...] = ()

    schema_version: ClassVar[int] = WORKFLOW_COMPLETION_SCHEMA_VERSION
    completion_type: ClassVar[str] = "active_reply"

    def __post_init__(self) -> None:
        """Freeze the captured consumption and proposal batches."""

        consumed = _message_log_ids(self.consumed_message_log_ids)
        actions = ExternalActionIntentBatch(tuple(self.external_action_intents))
        object.__setattr__(self, "consumed_message_log_ids", consumed)
        object.__setattr__(self, "external_action_intents", actions.intents)

    def to_payload(self) -> dict[str, Any]:
        """Encode this completion as strict versioned mailbox payload data."""

        return {
            "schema_version": self.schema_version,
            "completion_type": self.completion_type,
            "consumed_message_log_ids": list(self.consumed_message_log_ids),
            "external_actions": ExternalActionIntentBatch(
                self.external_action_intents
            ).to_payload(),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ActiveReplyCompletionResult:
        """Decode an active-reply completion and reject every extra field."""

        values = _decode_completion_header(
            payload,
            fields=_ACTIVE_REPLY_FIELDS,
            completion_type=cls.completion_type,
        )
        return cls(
            consumed_message_log_ids=_decode_message_log_ids(
                values.get("consumed_message_log_ids")
            ),
            external_action_intents=_decode_embedded_actions(
                values.get("external_actions")
            ).intents,
        )

    def to_json(self) -> str:
        """Encode this completion as canonical JSON."""

        return _canonical_json(self.to_payload())

    @classmethod
    def from_json(cls, payload: str) -> ActiveReplyCompletionResult:
        """Decode an active-reply completion from strict JSON."""

        return cls.from_payload(_json_object(payload, field_name="active_reply_completion"))


@dataclass(slots=True, frozen=True)
class ReviewCompletionResult:
    """Typed durable result returned by a review workflow effect.

    Exactly one terminal scheduling branch is required: enter active chat, or
    return idle with a relative next-review outcome.
    """

    enter_active_chat: bool
    next_review_outcome: ReviewNextReviewOutcome | None
    consumed_message_log_ids: tuple[int, ...] = ()
    external_action_intents: tuple[ExternalActionIntent, ...] = ()

    schema_version: ClassVar[int] = WORKFLOW_COMPLETION_SCHEMA_VERSION
    completion_type: ClassVar[str] = "review"

    def __post_init__(self) -> None:
        """Validate the exclusive state branch and freeze ordered inputs."""

        if not isinstance(self.enter_active_chat, bool):
            raise WorkflowCompletionCodecError("enter_active_chat must be a boolean")
        if self.enter_active_chat == (self.next_review_outcome is not None):
            raise WorkflowCompletionCodecError(
                "review completion requires exactly one of enter_active_chat or "
                "next_review_outcome"
            )
        if self.next_review_outcome is not None and not isinstance(
            self.next_review_outcome,
            ReviewNextReviewOutcome,
        ):
            raise WorkflowCompletionCodecError(
                "next_review_outcome must be a ReviewNextReviewOutcome"
            )
        consumed = _message_log_ids(self.consumed_message_log_ids)
        actions = ExternalActionIntentBatch(tuple(self.external_action_intents))
        object.__setattr__(self, "consumed_message_log_ids", consumed)
        object.__setattr__(self, "external_action_intents", actions.intents)

    def to_payload(self) -> dict[str, Any]:
        """Encode this completion without actor-owned fences or absolute time."""

        return {
            "schema_version": self.schema_version,
            "completion_type": self.completion_type,
            "consumed_message_log_ids": list(self.consumed_message_log_ids),
            "external_actions": ExternalActionIntentBatch(
                self.external_action_intents
            ).to_payload(),
            "enter_active_chat": self.enter_active_chat,
            "next_review_outcome": (
                self.next_review_outcome.to_payload()
                if self.next_review_outcome is not None
                else None
            ),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ReviewCompletionResult:
        """Decode a review completion and enforce its exclusive branch."""

        values = _decode_completion_header(
            payload,
            fields=_REVIEW_FIELDS,
            completion_type=cls.completion_type,
        )
        enter_active_chat = values.get("enter_active_chat")
        if not isinstance(enter_active_chat, bool):
            raise WorkflowCompletionCodecError("enter_active_chat must be a boolean")
        raw_outcome = values.get("next_review_outcome")
        if raw_outcome is None:
            outcome = None
        elif isinstance(raw_outcome, Mapping):
            outcome = ReviewNextReviewOutcome.from_payload(raw_outcome)
        else:
            raise WorkflowCompletionCodecError("next_review_outcome must be an object or null")
        return cls(
            enter_active_chat=enter_active_chat,
            next_review_outcome=outcome,
            consumed_message_log_ids=_decode_message_log_ids(
                values.get("consumed_message_log_ids")
            ),
            external_action_intents=_decode_embedded_actions(
                values.get("external_actions")
            ).intents,
        )

    def to_json(self) -> str:
        """Encode this completion as canonical JSON."""

        return _canonical_json(self.to_payload())

    @classmethod
    def from_json(cls, payload: str) -> ReviewCompletionResult:
        """Decode a review completion from strict JSON."""

        return cls.from_payload(_json_object(payload, field_name="review_completion"))


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapCompletionResult:
    """Typed curve correction returned after an active-chat handoff."""

    disposition: ActiveChatBootstrapDisposition
    reason: str

    schema_version: ClassVar[int] = WORKFLOW_COMPLETION_SCHEMA_VERSION
    completion_type: ClassVar[str] = "active_chat_bootstrap"

    def __post_init__(self) -> None:
        """Validate the bounded bootstrap decision."""

        try:
            disposition = ActiveChatBootstrapDisposition(self.disposition)
        except (TypeError, ValueError) as exc:
            raise WorkflowCompletionCodecError(
                f"unsupported active-chat bootstrap disposition: {self.disposition!r}"
            ) from exc
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, field_name="bootstrap.reason"),
        )

    def to_payload(self) -> dict[str, Any]:
        """Encode the immutable bootstrap result."""

        return {
            "schema_version": self.schema_version,
            "completion_type": self.completion_type,
            "disposition": self.disposition.value,
            "reason": self.reason,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> ActiveChatBootstrapCompletionResult:
        """Decode one strict active-chat bootstrap completion."""

        values = _decode_completion_header(
            payload,
            fields=_ACTIVE_CHAT_BOOTSTRAP_FIELDS,
            completion_type=cls.completion_type,
        )
        raw_disposition = values.get("disposition")
        if not isinstance(raw_disposition, str):
            raise WorkflowCompletionCodecError(
                "active_chat_bootstrap_completion.disposition must be a string"
            )
        try:
            disposition = ActiveChatBootstrapDisposition(raw_disposition)
        except ValueError as exc:
            raise WorkflowCompletionCodecError(
                "unsupported active-chat bootstrap disposition: "
                f"{raw_disposition!r}"
            ) from exc
        return cls(
            disposition=disposition,
            reason=_required_text(
                values.get("reason"),
                field_name="active_chat_bootstrap_completion.reason",
            ),
        )


@dataclass(slots=True, frozen=True)
class ActiveChatRoundCompletionResult:
    """Typed result from a single frozen active-chat input round."""

    outcome: ActiveChatRoundOutcome
    interest_delta: float
    reason: str
    consumed_message_log_ids: tuple[int, ...] = ()
    external_action_intents: tuple[ExternalActionIntent, ...] = ()

    schema_version: ClassVar[int] = WORKFLOW_COMPLETION_SCHEMA_VERSION
    completion_type: ClassVar[str] = "active_chat_round"

    def __post_init__(self) -> None:
        """Validate bounded, operation-local result facts."""

        try:
            outcome = ActiveChatRoundOutcome(self.outcome)
        except (TypeError, ValueError) as exc:
            raise WorkflowCompletionCodecError(
                f"unsupported active-chat round outcome: {self.outcome!r}"
            ) from exc
        delta = _number(
            self.interest_delta,
            field_name="active_chat_round_completion.interest_delta",
        )
        if not math.isfinite(delta) or delta < -100.0 or delta > 100.0:
            raise WorkflowCompletionCodecError(
                "active_chat_round_completion.interest_delta must be finite "
                "and within [-100, 100]"
            )
        consumed = _message_log_ids(self.consumed_message_log_ids)
        actions = ExternalActionIntentBatch(tuple(self.external_action_intents))
        if outcome is ActiveChatRoundOutcome.RETRY and (
            consumed or actions.intents
        ):
            raise WorkflowCompletionCodecError(
                "retry active-chat rounds cannot consume messages or propose actions"
            )
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "interest_delta", delta)
        object.__setattr__(
            self,
            "reason",
            _required_text(
                self.reason,
                field_name="active_chat_round_completion.reason",
            ),
        )
        object.__setattr__(self, "consumed_message_log_ids", consumed)
        object.__setattr__(self, "external_action_intents", actions.intents)

    def to_payload(self) -> dict[str, Any]:
        """Encode this result without actor-owned fences or timestamps."""

        return {
            "schema_version": self.schema_version,
            "completion_type": self.completion_type,
            "consumed_message_log_ids": list(self.consumed_message_log_ids),
            "external_actions": ExternalActionIntentBatch(
                self.external_action_intents
            ).to_payload(),
            "outcome": self.outcome.value,
            "interest_delta": self.interest_delta,
            "reason": self.reason,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> ActiveChatRoundCompletionResult:
        """Decode one strict active-chat round completion."""

        values = _decode_completion_header(
            payload,
            fields=_ACTIVE_CHAT_ROUND_FIELDS,
            completion_type=cls.completion_type,
        )
        raw_outcome = values.get("outcome")
        if not isinstance(raw_outcome, str):
            raise WorkflowCompletionCodecError(
                "active_chat_round_completion.outcome must be a string"
            )
        try:
            outcome = ActiveChatRoundOutcome(raw_outcome)
        except ValueError as exc:
            raise WorkflowCompletionCodecError(
                f"unsupported active-chat round outcome: {raw_outcome!r}"
            ) from exc
        return cls(
            outcome=outcome,
            interest_delta=_number(
                values.get("interest_delta"),
                field_name="active_chat_round_completion.interest_delta",
            ),
            reason=_required_text(
                values.get("reason"),
                field_name="active_chat_round_completion.reason",
            ),
            consumed_message_log_ids=_decode_message_log_ids(
                values.get("consumed_message_log_ids")
            ),
            external_action_intents=_decode_embedded_actions(
                values.get("external_actions")
            ).intents,
        )


def encode_external_action_intent_batch(
    intents: Iterable[ExternalActionIntent],
) -> dict[str, Any]:
    """Encode operation-global external action intents as a versioned batch."""

    return ExternalActionIntentBatch(tuple(intents)).to_payload()


def decode_external_action_intent_batch(
    payload: Mapping[str, Any],
) -> tuple[ExternalActionIntent, ...]:
    """Decode and validate one versioned external action intent batch."""

    return ExternalActionIntentBatch.from_payload(payload).intents


def _decode_external_action_intent(value: object, *, index: int) -> ExternalActionIntent:
    field_name = f"external_actions.intents[{index}]"
    values = _strict_object(value, _INTENT_FIELDS, field_name=field_name)
    proposal_id = _required_text(
        values.get("proposal_id"),
        field_name=f"{field_name}.proposal_id",
    )
    ordinal = _nonnegative_int(
        values.get("action_ordinal"),
        field_name=f"{field_name}.action_ordinal",
    )
    raw_kind = values.get("kind")
    if not isinstance(raw_kind, str):
        raise WorkflowCompletionCodecError(f"{field_name}.kind must be a string")
    try:
        kind = ExternalActionKind(raw_kind)
    except ValueError as exc:
        raise WorkflowCompletionCodecError(
            f"unsupported {field_name}.kind: {raw_kind!r}"
        ) from exc
    payload = _plain_json_object(
        values.get("payload"),
        field_name=f"{field_name}.payload",
    )
    _reject_reserved_runtime_fields(payload, path=f"{field_name}.payload")
    try:
        return ExternalActionIntent(
            kind=kind,
            tool_call_id=proposal_id,
            action_ordinal=ordinal,
            payload=payload,
        )
    except (TypeError, ValueError) as exc:
        raise WorkflowCompletionCodecError(f"invalid {field_name}: {exc}") from exc


def _validate_external_action_intents(
    intents: tuple[ExternalActionIntent, ...],
) -> None:
    proposal_ids: set[str] = set()
    for expected_ordinal, intent in enumerate(intents):
        if not isinstance(intent, ExternalActionIntent):
            raise WorkflowCompletionCodecError(
                f"external action intent {expected_ordinal} has an invalid type"
            )
        proposal_id = _required_text(
            intent.tool_call_id,
            field_name=f"external_actions.intents[{expected_ordinal}].proposal_id",
        )
        if proposal_id in proposal_ids:
            raise WorkflowCompletionCodecError(
                f"duplicate external action proposal_id: {proposal_id!r}"
            )
        proposal_ids.add(proposal_id)
        if intent.action_ordinal != expected_ordinal:
            raise WorkflowCompletionCodecError(
                "external action ordinals must be operation-global, contiguous, and "
                f"ordered from zero; expected {expected_ordinal}, got "
                f"{intent.action_ordinal}"
            )
        payload = _plain_json_object(
            intent.payload,
            field_name=f"external_actions.intents[{expected_ordinal}].payload",
        )
        _reject_reserved_runtime_fields(
            payload,
            path=f"external_actions.intents[{expected_ordinal}].payload",
        )


def _decode_completion_header(
    payload: Mapping[str, Any],
    *,
    fields: frozenset[str],
    completion_type: str,
) -> dict[str, Any]:
    values = _strict_object(payload, fields, field_name=f"{completion_type}_completion")
    _require_schema_version(
        values.get("schema_version"),
        expected=WORKFLOW_COMPLETION_SCHEMA_VERSION,
        field_name=f"{completion_type}_completion.schema_version",
    )
    actual_type = values.get("completion_type")
    if actual_type != completion_type:
        raise WorkflowCompletionCodecError(
            f"expected completion_type {completion_type!r}, got {actual_type!r}"
        )
    return values


def _decode_embedded_actions(value: object) -> ExternalActionIntentBatch:
    if not isinstance(value, Mapping):
        raise WorkflowCompletionCodecError("external_actions must be an object")
    return ExternalActionIntentBatch.from_payload(value)


def _decode_message_log_ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise WorkflowCompletionCodecError("consumed_message_log_ids must be an array")
    return _message_log_ids(value)


def _message_log_ids(values: Sequence[object]) -> tuple[int, ...]:
    normalized: list[int] = []
    seen: set[int] = set()
    for index, value in enumerate(values):
        message_log_id = _positive_int(
            value,
            field_name=f"consumed_message_log_ids[{index}]",
        )
        if message_log_id in seen:
            raise WorkflowCompletionCodecError(
                f"duplicate consumed message log id: {message_log_id}"
            )
        seen.add(message_log_id)
        normalized.append(message_log_id)
    return tuple(normalized)


def _review_outcome_kind(value: object) -> ReviewNextReviewOutcomeKind:
    if not isinstance(value, str):
        raise WorkflowCompletionCodecError("next_review_outcome.kind must be a string")
    try:
        return ReviewNextReviewOutcomeKind(value)
    except ValueError as exc:
        raise WorkflowCompletionCodecError(
            f"unsupported next review outcome kind: {value!r}"
        ) from exc


def _strict_object(
    value: object,
    expected_fields: frozenset[str],
    *,
    field_name: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkflowCompletionCodecError(f"{field_name} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise WorkflowCompletionCodecError(f"{field_name} keys must be strings")
    values = {str(key): item for key, item in value.items()}
    actual_fields = set(values)
    missing = sorted(expected_fields - actual_fields)
    unexpected = sorted(actual_fields - expected_fields)
    if missing or unexpected:
        details: list[str] = []
        if missing:
            details.append("missing fields: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected fields: " + ", ".join(unexpected))
        raise WorkflowCompletionCodecError(f"{field_name} has " + "; ".join(details))
    return values


def _require_schema_version(value: object, *, expected: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WorkflowCompletionCodecError(f"{field_name} must be an integer")
    if value != expected:
        raise WorkflowCompletionCodecError(
            f"unsupported {field_name}: expected {expected}, got {value}"
        )


def _reject_reserved_runtime_fields(value: Mapping[str, Any], *, path: str) -> None:
    for key, item in value.items():
        item_path = f"{path}.{key}"
        if key in _RUNTIME_RESERVED_FIELDS:
            raise WorkflowCompletionCodecError(
                f"external action payload contains runtime-reserved field: {item_path}"
            )
        if isinstance(item, Mapping):
            _reject_reserved_runtime_fields(item, path=item_path)
        elif isinstance(item, list):
            for index, nested in enumerate(item):
                if isinstance(nested, Mapping):
                    _reject_reserved_runtime_fields(
                        nested,
                        path=f"{item_path}[{index}]",
                    )


def _plain_json_object(value: object, *, field_name: str) -> dict[str, Any]:
    plain = _plain_json(value, path=field_name)
    if not isinstance(plain, dict):
        raise WorkflowCompletionCodecError(f"{field_name} must be an object")
    return plain


def _plain_json(value: object, *, path: str) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise WorkflowCompletionCodecError(f"{path} keys must be strings")
        return {
            str(key): _plain_json(item, path=f"{path}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _plain_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, float) and not math.isfinite(value):
        raise WorkflowCompletionCodecError(f"{path} numbers must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise WorkflowCompletionCodecError(f"{path} must contain only JSON-compatible values")


def _json_object(payload: str, *, field_name: str) -> dict[str, Any]:
    if not isinstance(payload, str):
        raise WorkflowCompletionCodecError(f"{field_name} JSON must be a string")
    try:
        value = json.loads(payload, object_pairs_hook=_object_without_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise WorkflowCompletionCodecError(f"{field_name} contains invalid JSON") from exc
    if not isinstance(value, dict):
        raise WorkflowCompletionCodecError(f"{field_name} JSON must contain an object")
    return value


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise WorkflowCompletionCodecError(f"duplicate JSON field: {key!r}")
        result[key] = value
    return result


def _canonical_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise WorkflowCompletionCodecError("workflow completion is not valid JSON") from exc


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise WorkflowCompletionCodecError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise WorkflowCompletionCodecError(f"{field_name} must not be empty")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise WorkflowCompletionCodecError(f"{field_name} must be a string")
    return value.strip()


def _number(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowCompletionCodecError(f"{field_name} must be a number")
    return float(value)


def _optional_number(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    return _number(value, field_name=field_name)


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    number = _number(value, field_name=field_name)
    if not math.isfinite(number) or number < 0:
        raise WorkflowCompletionCodecError(f"{field_name} must be finite and non-negative")
    return number


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WorkflowCompletionCodecError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WorkflowCompletionCodecError(f"{field_name} must be a non-negative integer")
    return value


__all__ = [
    "EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION",
    "WORKFLOW_COMPLETION_SCHEMA_VERSION",
    "ActiveChatBootstrapCompletionResult",
    "ActiveChatBootstrapDisposition",
    "ActiveChatRoundCompletionResult",
    "ActiveChatRoundOutcome",
    "ActiveReplyCompletionResult",
    "ExternalActionIntentBatch",
    "ReviewCompletionResult",
    "ReviewNextReviewOutcome",
    "ReviewNextReviewOutcomeKind",
    "WorkflowCompletionCodecError",
    "decode_external_action_intent_batch",
    "encode_external_action_intent_batch",
]
