"""Pure deterministic identity rules for durable ``ReviewDue`` delivery."""

from __future__ import annotations

import json
import uuid

from shinbot.agent.runtime.session_actor.aggregate import SessionKey

REVIEW_DUE_EVENT_KIND = "ReviewDue"
REVIEW_DUE_EVENT_SOURCE = "durable_review_due_scanner"

_REVIEW_DUE_NAMESPACE = uuid.UUID("b6498305-e43b-5fba-84d6-c0aee625eb7e")


def review_due_event_id(
    *,
    key: SessionKey,
    plan_id: str,
    plan_revision: int,
    ownership_generation: int,
    delivery_cycle: int = 0,
) -> str:
    """Return the deterministic mailbox identity for one due delivery cycle.

    Cycle zero deliberately retains the original v1 identity byte-for-byte so
    mailbox debt written before delivery cycles existed remains idempotent.
    Later cycles use a v2 identity that includes the monotonic cycle fence.

    Args:
        key: Profile-scoped actor session identity.
        plan_id: Current durable review plan identifier.
        plan_revision: Current durable review plan revision.
        ownership_generation: Active actor ownership generation.
        delivery_cycle: Zero-based semantic due-delivery cycle.

    Returns:
        A stable event identifier for the exact delivery fence.

    Raises:
        ValueError: If any identity fence is invalid.
    """

    normalized_plan_id = str(plan_id or "").strip()
    if not normalized_plan_id:
        raise ValueError("plan_id must not be empty")
    normalized_revision = _positive_int(plan_revision, field_name="plan_revision")
    normalized_generation = _positive_int(
        ownership_generation,
        field_name="ownership_generation",
    )
    normalized_cycle = _nonnegative_int(
        delivery_cycle,
        field_name="delivery_cycle",
    )
    identity_parts: list[object] = [
        key.profile_id,
        key.session_id,
        normalized_plan_id,
        normalized_revision,
        normalized_generation,
    ]
    identity_version = 1
    if normalized_cycle > 0:
        identity_parts.append(normalized_cycle)
        identity_version = 2
    identity = json.dumps(
        identity_parts,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = uuid.uuid5(_REVIEW_DUE_NAMESPACE, identity).hex
    return f"review-due:v{identity_version}:{digest}"


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


__all__ = [
    "REVIEW_DUE_EVENT_KIND",
    "REVIEW_DUE_EVENT_SOURCE",
    "review_due_event_id",
]
