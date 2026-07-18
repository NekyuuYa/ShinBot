"""Contracts for a future lossless adapter ingress pause-and-drain boundary.

The types in this module describe one *process-local* adapter participant. A
production controller still needs durable process membership and must collect a
positive receipt from every process that can accept the named adapter ingress.
No default adapter implementation exists because an in-memory callback flag or
queue cannot prove a lossless cutover boundary.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable


class AdapterIngressPauseDeliveryGuarantee(StrEnum):
    """Where an adapter preserves events admitted after its local pause."""

    DURABLE_BUFFER = "durable_buffer"
    UPSTREAM_ACK = "upstream_ack"


class AdapterIngressPauseStatus(StrEnum):
    """Result of one local adapter callback drain observation."""

    QUIESCENT = "quiescent"
    TIMED_OUT = "timed_out"
    CURRENT_CALLBACK_ACTIVE = "current_callback_active"
    PAUSE_LOST = "pause_lost"


class AdapterIngressPauseSupportStatus(StrEnum):
    """Manager-visible availability of a local pause participant."""

    AVAILABLE = "available"
    MISSING_INSTANCE = "missing_instance"
    NOT_RUNNING = "not_running"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


@dataclass(slots=True, frozen=True)
class AdapterIngressPauseRequest:
    """Immutable local request bound to one adapter/session cutover epoch."""

    adapter_instance_id: str
    legacy_session_id: str
    cutover_id: str
    cutover_epoch: int

    def __post_init__(self) -> None:
        """Normalize identifiers before a participant accepts a pause request."""

        adapter_instance_id = _required_identifier(
            self.adapter_instance_id,
            "adapter_instance_id",
        )
        legacy_session_id = _required_identifier(
            self.legacy_session_id,
            "legacy_session_id",
        )
        cutover_id = _required_identifier(self.cutover_id, "cutover_id")
        cutover_epoch = _positive_integer(self.cutover_epoch, "cutover_epoch")
        object.__setattr__(self, "adapter_instance_id", adapter_instance_id)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "cutover_id", cutover_id)
        object.__setattr__(self, "cutover_epoch", cutover_epoch)


@dataclass(slots=True, frozen=True)
class AdapterIngressPauseTicket:
    """Opaque local authority for one adapter process pause epoch.

    ``token`` must never enter durable cutover evidence, logs, or API payloads.
    It is only a local capability used to await or resume this exact pause.
    """

    request: AdapterIngressPauseRequest
    participant_id: str
    participant_epoch: int
    token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Validate the local participant identity and opaque capability."""

        if not isinstance(self.request, AdapterIngressPauseRequest):
            raise TypeError("request must be an AdapterIngressPauseRequest")
        participant_id = _required_identifier(self.participant_id, "participant_id")
        participant_epoch = _positive_integer(self.participant_epoch, "participant_epoch")
        token = _required_identifier(self.token, "token")
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "participant_epoch", participant_epoch)
        object.__setattr__(self, "token", token)


@dataclass(slots=True, frozen=True)
class AdapterIngressPauseReceipt:
    """One local adapter participant's callback drain result."""

    ticket: AdapterIngressPauseTicket
    status: AdapterIngressPauseStatus
    in_flight_callback_count: int = 0
    buffered_event_count: int = 0

    def __post_init__(self) -> None:
        """Reject negative counters and normalize the receipt status."""

        if not isinstance(self.ticket, AdapterIngressPauseTicket):
            raise TypeError("ticket must be an AdapterIngressPauseTicket")
        status = AdapterIngressPauseStatus(self.status)
        in_flight_callback_count = _non_negative_integer(
            self.in_flight_callback_count,
            "in_flight_callback_count",
        )
        buffered_event_count = _non_negative_integer(
            self.buffered_event_count,
            "buffered_event_count",
        )
        if (
            status is AdapterIngressPauseStatus.QUIESCENT
            and in_flight_callback_count != 0
        ):
            raise ValueError("a quiescent receipt cannot retain in-flight callbacks")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "in_flight_callback_count", in_flight_callback_count)
        object.__setattr__(self, "buffered_event_count", buffered_event_count)

    @property
    def quiescent(self) -> bool:
        """Return whether local pre-pause callbacks have drained."""

        return self.status is AdapterIngressPauseStatus.QUIESCENT


@runtime_checkable
class AdapterIngressPauseParticipant(Protocol):
    """A lossless local participant implemented by one adapter process.

    Implementations must stop local callback admission for the requested base
    session before returning a ticket. Events observed after the pause must be
    retained in a durable buffer or under a platform acknowledgement/flow
    control contract until resume. A process-local memory queue is insufficient.
    """

    @property
    def adapter_instance_id(self) -> str:
        """Return the adapter instance this participant owns."""

    @property
    def participant_id(self) -> str:
        """Return a process-incarnation identifier, not a mutable worker name."""

    @property
    def delivery_guarantee(self) -> AdapterIngressPauseDeliveryGuarantee:
        """Return how post-pause events survive until a future resume."""

    def pause_ingress(
        self,
        request: AdapterIngressPauseRequest,
    ) -> AdapterIngressPauseTicket:
        """Close local callback admission for one exact adapter/session epoch."""

    async def await_ingress_quiescent(
        self,
        ticket: AdapterIngressPauseTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> AdapterIngressPauseReceipt:
        """Wait for all pre-pause callback work in this process to exit."""

    def resume_ingress(self, ticket: AdapterIngressPauseTicket) -> bool:
        """Release one local pause after a controller chose its next owner."""


@dataclass(slots=True, frozen=True)
class AdapterIngressPauseSupport:
    """One AdapterManager instance's read-only pause-capability assessment."""

    adapter_instance_id: str
    status: AdapterIngressPauseSupportStatus
    participant_id: str = ""
    delivery_guarantee: AdapterIngressPauseDeliveryGuarantee | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        """Require exact metadata for a usable local participant."""

        adapter_instance_id = _required_identifier(
            self.adapter_instance_id,
            "adapter_instance_id",
        )
        status = AdapterIngressPauseSupportStatus(self.status)
        participant_id = str(self.participant_id or "").strip()
        reason = str(self.reason or "").strip()
        delivery_guarantee = self.delivery_guarantee
        if delivery_guarantee is not None:
            delivery_guarantee = AdapterIngressPauseDeliveryGuarantee(
                delivery_guarantee
            )
        if status is AdapterIngressPauseSupportStatus.AVAILABLE:
            if not participant_id or delivery_guarantee is None:
                raise ValueError(
                    "available adapter pause support requires participant metadata"
                )
        elif participant_id or delivery_guarantee is not None:
            raise ValueError("unavailable adapter pause support cannot retain participant metadata")
        object.__setattr__(self, "adapter_instance_id", adapter_instance_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "delivery_guarantee", delivery_guarantee)
        object.__setattr__(self, "reason", reason)

    @property
    def available(self) -> bool:
        """Return whether the local running adapter exposes a valid participant."""

        return self.status is AdapterIngressPauseSupportStatus.AVAILABLE


@dataclass(slots=True, frozen=True)
class AdapterIngressPauseSupportInventory:
    """Exact local capability assessment for all adapters of one future cutover."""

    supports: tuple[AdapterIngressPauseSupport, ...]

    def __post_init__(self) -> None:
        """Keep one deterministic support entry per adapter instance."""

        supports = tuple(self.supports)
        if not supports:
            raise ValueError("supports must not be empty")
        if any(not isinstance(item, AdapterIngressPauseSupport) for item in supports):
            raise TypeError("supports must contain AdapterIngressPauseSupport values")
        instance_ids = tuple(item.adapter_instance_id for item in supports)
        if len(set(instance_ids)) != len(instance_ids):
            raise ValueError("supports cannot repeat an adapter_instance_id")
        object.__setattr__(
            self,
            "supports",
            tuple(sorted(supports, key=lambda item: item.adapter_instance_id)),
        )

    @property
    def all_available(self) -> bool:
        """Return whether every named local adapter exposes a valid participant."""

        return all(support.available for support in self.supports)

    @property
    def unavailable_instance_ids(self) -> tuple[str, ...]:
        """Return local adapters that still block pause-and-drain preparation."""

        return tuple(
            support.adapter_instance_id
            for support in self.supports
            if not support.available
        )


def new_adapter_ingress_pause_token() -> str:
    """Create one opaque local ticket token for a participant implementation."""

    return uuid.uuid4().hex


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one required identifier without accepting whitespace-only input."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_integer(value: object, field_name: str) -> int:
    """Validate one positive integer epoch value."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _non_negative_integer(value: object, field_name: str) -> int:
    """Validate one finite non-negative integer counter."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


__all__ = [
    "AdapterIngressPauseDeliveryGuarantee",
    "AdapterIngressPauseParticipant",
    "AdapterIngressPauseReceipt",
    "AdapterIngressPauseRequest",
    "AdapterIngressPauseStatus",
    "AdapterIngressPauseSupport",
    "AdapterIngressPauseSupportInventory",
    "AdapterIngressPauseSupportStatus",
    "AdapterIngressPauseTicket",
    "new_adapter_ingress_pause_token",
]
