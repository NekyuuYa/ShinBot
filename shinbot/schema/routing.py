"""Stable message routing state values stored in message_logs."""

from __future__ import annotations

from enum import StrEnum


class MessageRoutingStatus(StrEnum):
    """Lifecycle status for the core routing layer."""

    PENDING = "pending"
    DISPATCHED = "dispatched"
    SKIPPED = "skipped"


class MessageRoutingSkipReason(StrEnum):
    """Reasons why routing completed without dispatching to a target."""

    EXPIRED_MESSAGE = "expired_message"
    NO_ROUTE_MATCHED = "no_route_matched"
    SESSION_MUTED = "session_muted"
    INTERCEPTOR_BLOCKED = "interceptor_blocked"
    WAIT_FOR_INPUT = "wait_for_input"


def routing_status_value(status: MessageRoutingStatus | str) -> str:
    """Return the persisted string value for a routing status."""
    return status.value if isinstance(status, MessageRoutingStatus) else status


def routing_skip_reason_value(reason: MessageRoutingSkipReason | str) -> str:
    """Return the persisted string value for a routing skip reason."""
    return reason.value if isinstance(reason, MessageRoutingSkipReason) else reason


__all__ = [
    "MessageRoutingSkipReason",
    "MessageRoutingStatus",
    "routing_skip_reason_value",
    "routing_status_value",
]
