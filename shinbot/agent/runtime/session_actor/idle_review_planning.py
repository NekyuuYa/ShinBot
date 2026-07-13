"""Versioned durable input for Actor v2 idle-review planning.

The legacy runtime built the planner prompt from mutable coordinator state.
Actor v2 instead records a compact, actor-derived snapshot descriptor with the
exit effect. A later read-only projector can use the descriptor's watermark to
construct model context without consulting the legacy scheduler.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

IDLE_REVIEW_PLANNING_INPUT_VERSION = 1
"""Current durable schema version for an idle-review planning input."""

_INPUT_FIELDS = frozenset(
    {
        "version",
        "input_watermark",
        "active_epoch",
        "activity_generation",
        "trigger",
        "active_chat",
    }
)
_ACTIVE_CHAT_FIELDS = frozenset(
    {
        "interest_value",
        "entered_at",
        "last_message_at",
        "tick_count",
        "bootstrap_disposition",
    }
)
_MAX_TEXT_LENGTH = 256


class IdleReviewPlanningInputError(ValueError):
    """Raised when a persisted idle-review planning input is not canonical."""


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningInput:
    """Actor-derived descriptor for one idle-review planner invocation.

    This deliberately carries no free-form prompt content. ``input_watermark``
    fences later read-only ledger projection, while the compact active-chat
    values describe the state that requested the exit.
    """

    input_watermark: int
    active_epoch: int
    activity_generation: int
    trigger: str
    active_chat_interest: float | None = None
    active_chat_entered_at: float | None = None
    active_chat_last_message_at: float | None = None
    active_chat_tick_count: int = 0
    active_chat_bootstrap_disposition: str = ""
    version: int = IDLE_REVIEW_PLANNING_INPUT_VERSION

    def __post_init__(self) -> None:
        """Validate one canonical, bounded planner-input descriptor."""

        if self.version != IDLE_REVIEW_PLANNING_INPUT_VERSION:
            raise IdleReviewPlanningInputError(
                f"unsupported idle review planning input version: {self.version!r}"
            )
        for field_name in (
            "input_watermark",
            "active_epoch",
            "activity_generation",
            "active_chat_tick_count",
        ):
            _nonnegative_int(getattr(self, field_name), field_name=field_name)
        object.__setattr__(
            self,
            "trigger",
            _required_text(self.trigger, field_name="trigger"),
        )
        for field_name in (
            "active_chat_interest",
            "active_chat_entered_at",
            "active_chat_last_message_at",
        ):
            value = _optional_nonnegative_finite(
                getattr(self, field_name),
                field_name=field_name,
            )
            object.__setattr__(self, field_name, value)
        object.__setattr__(
            self,
            "active_chat_bootstrap_disposition",
            _optional_text(
                self.active_chat_bootstrap_disposition,
                field_name="active_chat_bootstrap_disposition",
            ),
        )

    @classmethod
    def from_active_chat_exit(
        cls,
        *,
        input_watermark: int,
        active_epoch: int,
        activity_generation: int,
        trigger: str,
        active_chat_state: Mapping[str, Any],
    ) -> IdleReviewPlanningInput:
        """Build a descriptor from actor-owned aggregate state.

        Unknown active-chat fields are deliberately omitted. They might be
        implementation details or user/model content and are not authority for
        a planner prompt.
        """

        return cls(
            input_watermark=input_watermark,
            active_epoch=active_epoch,
            activity_generation=activity_generation,
            trigger=trigger,
            active_chat_interest=_coerce_optional_nonnegative_finite(
                active_chat_state.get("interest_value")
            ),
            active_chat_entered_at=_coerce_optional_nonnegative_finite(
                active_chat_state.get("entered_at")
            ),
            active_chat_last_message_at=_coerce_optional_nonnegative_finite(
                active_chat_state.get("last_message_at")
            ),
            active_chat_tick_count=_coerce_nonnegative_int(
                active_chat_state.get("tick_count")
            ),
            active_chat_bootstrap_disposition=_coerce_optional_text(
                active_chat_state.get("bootstrap_disposition")
            ),
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> IdleReviewPlanningInput:
        """Decode the strict persisted input descriptor."""

        values = _strict_object(payload, fields=_INPUT_FIELDS, field_name="planning_input")
        active_chat = _strict_object(
            values["active_chat"],
            fields=_ACTIVE_CHAT_FIELDS,
            field_name="planning_input.active_chat",
        )
        return cls(
            version=_nonnegative_int(values["version"], field_name="version"),
            input_watermark=_nonnegative_int(
                values["input_watermark"],
                field_name="input_watermark",
            ),
            active_epoch=_nonnegative_int(values["active_epoch"], field_name="active_epoch"),
            activity_generation=_nonnegative_int(
                values["activity_generation"],
                field_name="activity_generation",
            ),
            trigger=_required_text(values["trigger"], field_name="trigger"),
            active_chat_interest=_optional_nonnegative_finite(
                active_chat["interest_value"],
                field_name="active_chat.interest_value",
            ),
            active_chat_entered_at=_optional_nonnegative_finite(
                active_chat["entered_at"],
                field_name="active_chat.entered_at",
            ),
            active_chat_last_message_at=_optional_nonnegative_finite(
                active_chat["last_message_at"],
                field_name="active_chat.last_message_at",
            ),
            active_chat_tick_count=_nonnegative_int(
                active_chat["tick_count"],
                field_name="active_chat.tick_count",
            ),
            active_chat_bootstrap_disposition=_optional_text(
                active_chat["bootstrap_disposition"],
                field_name="active_chat.bootstrap_disposition",
            ),
        )

    def to_payload(self) -> dict[str, object]:
        """Encode the immutable descriptor for the durable effect payload."""

        return {
            "version": self.version,
            "input_watermark": self.input_watermark,
            "active_epoch": self.active_epoch,
            "activity_generation": self.activity_generation,
            "trigger": self.trigger,
            "active_chat": {
                "interest_value": self.active_chat_interest,
                "entered_at": self.active_chat_entered_at,
                "last_message_at": self.active_chat_last_message_at,
                "tick_count": self.active_chat_tick_count,
                "bootstrap_disposition": self.active_chat_bootstrap_disposition,
            },
        }


def _strict_object(
    value: object,
    *,
    fields: frozenset[str],
    field_name: str,
) -> dict[str, Any]:
    """Copy one object only when it has exactly the declared field set."""

    if not isinstance(value, Mapping):
        raise IdleReviewPlanningInputError(f"{field_name} must be an object")
    values = {str(key): item for key, item in value.items()}
    actual = frozenset(values)
    if actual != fields:
        missing = ", ".join(sorted(fields - actual))
        extra = ", ".join(sorted(actual - fields))
        details = ", ".join(
            item
            for item in (
                f"missing={missing}" if missing else "",
                f"extra={extra}" if extra else "",
            )
            if item
        )
        raise IdleReviewPlanningInputError(
            f"{field_name} fields differ from schema: {details}"
        )
    return values


def _required_text(value: object, *, field_name: str) -> str:
    """Return bounded non-empty text without implicit conversion."""

    if not isinstance(value, str):
        raise IdleReviewPlanningInputError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise IdleReviewPlanningInputError(f"{field_name} must not be empty")
    if len(normalized) > _MAX_TEXT_LENGTH:
        raise IdleReviewPlanningInputError(f"{field_name} exceeds {_MAX_TEXT_LENGTH} chars")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    """Normalize bounded optional text without truthy coercion."""

    if value is None:
        return ""
    if not isinstance(value, str):
        raise IdleReviewPlanningInputError(f"{field_name} must be a string or null")
    normalized = value.strip()
    if len(normalized) > _MAX_TEXT_LENGTH:
        raise IdleReviewPlanningInputError(f"{field_name} exceeds {_MAX_TEXT_LENGTH} chars")
    return normalized


def _nonnegative_int(value: object, *, field_name: str) -> int:
    """Return a non-negative integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise IdleReviewPlanningInputError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _optional_nonnegative_finite(value: object, *, field_name: str) -> float | None:
    """Return a finite non-negative number or ``None``."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IdleReviewPlanningInputError(
            f"{field_name} must be a finite number or null"
        )
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise IdleReviewPlanningInputError(
            f"{field_name} must be a finite non-negative number"
        )
    return normalized


def _coerce_optional_nonnegative_finite(value: object) -> float | None:
    """Drop malformed aggregate details instead of exporting them to a prompt."""

    try:
        return _optional_nonnegative_finite(value, field_name="active_chat")
    except IdleReviewPlanningInputError:
        return None


def _coerce_nonnegative_int(value: object) -> int:
    """Drop malformed aggregate counters rather than leaking them to a prompt."""

    try:
        return _nonnegative_int(value, field_name="active_chat")
    except IdleReviewPlanningInputError:
        return 0


def _coerce_optional_text(value: object) -> str:
    """Drop malformed aggregate labels rather than leaking them to a prompt."""

    try:
        return _optional_text(value, field_name="active_chat")
    except IdleReviewPlanningInputError:
        return ""


__all__ = [
    "IDLE_REVIEW_PLANNING_INPUT_VERSION",
    "IdleReviewPlanningInput",
    "IdleReviewPlanningInputError",
]
