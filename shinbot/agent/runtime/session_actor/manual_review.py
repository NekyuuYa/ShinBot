"""Identity and payload validation for durable manual review admission."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey

MANUAL_REVIEW_EVENT_KIND = "ManualReviewRequested"
MANUAL_REVIEW_EVENT_SOURCE = "manual_review_admission"
MANUAL_REVIEW_REQUEST_VERSION = 1

_MANUAL_REVIEW_EVENT_NAMESPACE = uuid.UUID("2f42e20d-3869-55a4-8513-62475e35db3a")
_PAYLOAD_FIELDS = frozenset(
    {
        "version",
        "event_id",
        "session_key",
        "request_id",
        "ownership_generation",
        "plan_id",
        "plan_revision",
        "delivery_cycle",
        "requested_by",
        "reason",
    }
)


class ManualReviewRequestError(ValueError):
    """Raised when a manual review request is not a canonical durable input."""


class ManualReviewAdmissionRequiredError(ManualReviewRequestError):
    """Raised when a generic mailbox writer bypasses schedule admission."""


@dataclass(slots=True, frozen=True)
class ManualReviewRequest:
    """One operator request fenced to an exact Actor review schedule.

    ``request_id`` is caller-owned idempotency identity. The event identity
    includes every admission fence; the repository separately detects a prior
    request id before admitting new work, so an old request cannot silently
    rebase onto a later owner generation or replacement plan.
    """

    key: SessionKey
    request_id: str
    ownership_generation: int
    plan_id: str
    plan_revision: int
    delivery_cycle: int
    requested_by: str
    reason: str

    def __post_init__(self) -> None:
        """Normalize immutable request identity and validate schedule fences."""

        for field_name in ("request_id", "plan_id", "requested_by", "reason"):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise ManualReviewRequestError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, value)
        _positive_int(self.ownership_generation, "ownership_generation")
        _positive_int(self.plan_revision, "plan_revision")
        _nonnegative_int(self.delivery_cycle, "delivery_cycle")

    @property
    def event_id(self) -> str:
        """Return the stable mailbox identity for this explicit operator request."""

        return manual_review_event_id(
            self.key,
            request_id=self.request_id,
            ownership_generation=self.ownership_generation,
            plan_id=self.plan_id,
            plan_revision=self.plan_revision,
            delivery_cycle=self.delivery_cycle,
            requested_by=self.requested_by,
            reason=self.reason,
        )

    def to_payload(self) -> dict[str, Any]:
        """Serialize the complete, reducer-validated manual admission proof."""

        return {
            "version": MANUAL_REVIEW_REQUEST_VERSION,
            "event_id": self.event_id,
            "session_key": {
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
            },
            "request_id": self.request_id,
            "ownership_generation": self.ownership_generation,
            "plan_id": self.plan_id,
            "plan_revision": self.plan_revision,
            "delivery_cycle": self.delivery_cycle,
            "requested_by": self.requested_by,
            "reason": self.reason,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        event_id: str,
        key: SessionKey,
        ownership_generation: int,
    ) -> ManualReviewRequest:
        """Decode and verify a payload against its immutable mailbox envelope."""

        if not isinstance(payload, Mapping):
            raise ManualReviewRequestError("manual review payload must be an object")
        keys = frozenset(str(field) for field in payload)
        if keys != _PAYLOAD_FIELDS:
            raise ManualReviewRequestError("manual review payload fields changed")
        version = payload.get("version")
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != MANUAL_REVIEW_REQUEST_VERSION
        ):
            raise ManualReviewRequestError("manual review payload version changed")
        raw_key = payload.get("session_key")
        if not isinstance(raw_key, Mapping):
            raise ManualReviewRequestError("manual review session_key must be an object")
        payload_key = SessionKey(
            _required_text(raw_key, "profile_id"),
            _required_text(raw_key, "session_id"),
        )
        if payload_key != key:
            raise ManualReviewRequestError("manual review session_key changed")
        request = cls(
            key=key,
            request_id=_required_text(payload, "request_id"),
            ownership_generation=_required_positive_int(
                payload,
                "ownership_generation",
            ),
            plan_id=_required_text(payload, "plan_id"),
            plan_revision=_required_positive_int(payload, "plan_revision"),
            delivery_cycle=_required_nonnegative_int(payload, "delivery_cycle"),
            requested_by=_required_text(payload, "requested_by"),
            reason=_required_text(payload, "reason"),
        )
        if _required_text(payload, "event_id") != request.event_id:
            raise ManualReviewRequestError("manual review event_id changed")
        normalized_event_id = str(event_id or "").strip()
        if normalized_event_id != request.event_id:
            raise ManualReviewRequestError("manual review mailbox event_id changed")
        if request.ownership_generation != ownership_generation:
            raise ManualReviewRequestError(
                "manual review ownership_generation changed"
            )
        return request


def manual_review_event_id(
    key: SessionKey,
    *,
    request_id: str,
    ownership_generation: int,
    plan_id: str,
    plan_revision: int,
    delivery_cycle: int,
    requested_by: str,
    reason: str,
) -> str:
    """Return the deterministic identity for one exact admission proof."""

    normalized_request_id = str(request_id or "").strip()
    normalized_plan_id = str(plan_id or "").strip()
    normalized_requested_by = str(requested_by or "").strip()
    normalized_reason = str(reason or "").strip()
    if not normalized_request_id or not normalized_plan_id:
        raise ManualReviewRequestError("request_id and plan_id must not be empty")
    if not normalized_requested_by or not normalized_reason:
        raise ManualReviewRequestError("requested_by and reason must not be empty")
    normalized_generation = _positive_int(
        ownership_generation,
        "ownership_generation",
    )
    normalized_revision = _positive_int(plan_revision, "plan_revision")
    normalized_cycle = _nonnegative_int(delivery_cycle, "delivery_cycle")
    identity = _canonical_json(
        [
            key.profile_id,
            key.session_id,
            normalized_request_id,
            normalized_generation,
            normalized_plan_id,
            normalized_revision,
            normalized_cycle,
            normalized_requested_by,
            normalized_reason,
        ]
    )
    return (
        "manual-review-request:v1:"
        f"{uuid.uuid5(_MANUAL_REVIEW_EVENT_NAMESPACE, identity).hex}"
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def _required_text(payload: Mapping[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ManualReviewRequestError(f"{field_name} must be a non-empty string")
    return normalized


def _required_positive_int(payload: Mapping[str, object], field_name: str) -> int:
    value = payload.get(field_name)
    return _positive_int(value, field_name)


def _required_nonnegative_int(
    payload: Mapping[str, object],
    field_name: str,
) -> int:
    value = payload.get(field_name)
    return _nonnegative_int(value, field_name)


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ManualReviewRequestError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ManualReviewRequestError(
            f"{field_name} must be a non-negative integer"
        )
    return value


__all__ = [
    "MANUAL_REVIEW_EVENT_KIND",
    "MANUAL_REVIEW_EVENT_SOURCE",
    "MANUAL_REVIEW_REQUEST_VERSION",
    "ManualReviewAdmissionRequiredError",
    "ManualReviewRequest",
    "ManualReviewRequestError",
    "manual_review_event_id",
]
