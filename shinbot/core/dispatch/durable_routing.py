"""Core-owned contracts for recoverable message routing work."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.core.dispatch.agent_identity import SessionKey, SessionKeyFactory
from shinbot.schema.events import UnifiedEvent

MESSAGE_ROUTING_JOB_VERSION = 1
AGENT_ROUTE_OUTBOX_VERSION = 1
AGENT_ROUTE_MAILBOX_KIND = "MessageReceived"
AGENT_ROUTE_MAILBOX_SOURCE = "agent_route_outbox"
INGRESS_ROUTING_PAYLOAD_VERSION = 1
_INGRESS_JOB_NAMESPACE = uuid.UUID("de064f6a-c438-548a-8a07-e78d32ac3581")
_INGRESS_PAYLOAD_FIELDS = frozenset(
    {
        "version",
        "event",
        "adapter_instance_id",
        "adapter_platform",
        "message_xml",
        "trace_id",
        "observed_at",
        "base_session_id",
        "bot_id",
        "bot_binding_id",
        "bot_session_id",
        "fresh_at_ingress",
        "payload_digest",
    }
)


class _FrozenDict(dict[str, Any]):
    """JSON object that rejects mutation after durable identity creation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable routing payloads are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenList(list[Any]):
    """JSON array that rejects mutation after durable identity creation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable routing payloads are immutable")

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


def _freeze_json(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("durable routing payload numbers must be finite")
        return value
    if isinstance(value, Mapping):
        frozen: list[tuple[str, Any]] = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("durable routing payload keys must be strings")
            frozen.append((key, _freeze_json(item)))
        return _FrozenDict(frozen)
    if isinstance(value, (list, tuple)):
        return _FrozenList(_freeze_json(item) for item in value)
    raise TypeError(f"durable routing payload value is not JSON-compatible: {type(value)!r}")


class MessageRoutingJobStatus(StrEnum):
    """Durable lifecycle state for one message-routing job."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class AgentRouteOutboxStatus(StrEnum):
    """Durable lifecycle state for one Agent route delivery."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class IngressRoutingPayloadError(ValueError):
    """Raised when a persisted ingress replay payload is not canonical."""


@dataclass(slots=True, frozen=True)
class IngressRoutingPayload:
    """Canonical replay input for one accepted actor-owned message.

    The digest covers every field except ``payload_digest`` itself. Recovery
    reconstructs the same ``UnifiedEvent`` and message XML without consulting
    transient adapter callbacks or in-memory route context.
    """

    event: Mapping[str, Any]
    adapter_instance_id: str
    adapter_platform: str
    message_xml: str
    trace_id: str
    observed_at: float
    base_session_id: str
    bot_id: str
    bot_binding_id: str
    bot_session_id: str
    fresh_at_ingress: bool
    version: int = INGRESS_ROUTING_PAYLOAD_VERSION

    def __post_init__(self) -> None:
        """Normalize and validate the complete replay boundary."""

        if self.version != INGRESS_ROUTING_PAYLOAD_VERSION:
            raise IngressRoutingPayloadError(
                f"unsupported ingress routing payload version: {self.version}"
            )
        for field_name in (
            "adapter_instance_id",
            "adapter_platform",
            "message_xml",
            "trace_id",
            "base_session_id",
            "bot_id",
            "bot_binding_id",
            "bot_session_id",
        ):
            value = str(getattr(self, field_name) or "")
            if field_name != "message_xml":
                value = value.strip()
            object.__setattr__(self, field_name, value)
        for field_name in ("adapter_instance_id", "trace_id", "base_session_id"):
            if not getattr(self, field_name):
                raise IngressRoutingPayloadError(f"{field_name} must not be empty")
        if isinstance(self.observed_at, bool):
            raise IngressRoutingPayloadError("observed_at must be finite and non-negative")
        observed_at = float(self.observed_at)
        if not math.isfinite(observed_at) or observed_at < 0:
            raise IngressRoutingPayloadError("observed_at must be finite and non-negative")
        object.__setattr__(self, "observed_at", observed_at)
        if not isinstance(self.fresh_at_ingress, bool):
            raise IngressRoutingPayloadError("fresh_at_ingress must be a boolean")
        event = UnifiedEvent.model_validate(dict(self.event))
        event_payload = _canonical_json_object(event.model_dump(mode="json"))
        normalized_event = json.loads(event_payload)
        if not isinstance(normalized_event, dict):
            raise IngressRoutingPayloadError("event must serialize to a JSON object")
        if event.message_content != self.message_xml:
            raise IngressRoutingPayloadError(
                "message_xml does not match the serialized UnifiedEvent"
            )
        object.__setattr__(self, "event", _freeze_json(normalized_event))

    @property
    def session_key(self) -> SessionKey:
        """Return the canonical actor key captured by routing selection."""

        return SessionKeyFactory().create(
            bot_config_id=self.bot_id,
            bot_id=self.bot_id,
            bot_session_id=self.bot_session_id,
            base_session_id=self.base_session_id,
        )

    @property
    def payload_digest(self) -> str:
        """Return the SHA-256 digest of the canonical unsigned payload."""

        return _digest(_canonical_json_object(self._unsigned_payload()))

    @property
    def routing_job_id(self) -> str:
        """Return a version-independent logical id for platform ingress."""

        event = self.to_event()
        stable_event_id = ""
        if event.message is not None:
            stable_event_id = str(event.message.id or "").strip()
        if not stable_event_id:
            stable_event_id = str(event.id or event.sn or "").strip()
        if not stable_event_id:
            stable_event_id = _digest(
                _canonical_json_object(
                    {
                        "event": dict(self.event),
                        "message_xml": self.message_xml,
                    }
                )
            )
        identity = _canonical_json_array(
            [
                self.adapter_instance_id,
                self.adapter_platform,
                self.base_session_id,
                event.type,
                stable_event_id,
            ]
        )
        return f"message-routing:{uuid.uuid5(_INGRESS_JOB_NAMESPACE, identity).hex}"

    @property
    def idempotency_key(self) -> str:
        """Return the deterministic ingress idempotency key."""

        return self.routing_job_id.replace("message-routing:", "message-routing-idempotency:", 1)

    def to_event(self) -> UnifiedEvent:
        """Reconstruct the validated normalized event."""

        return UnifiedEvent.model_validate(dict(self.event))

    def has_same_ingress_identity(self, other: IngressRoutingPayload) -> bool:
        """Compare immutable platform/routing identity while ignoring receipt time."""

        return self._identity_payload() == other._identity_payload()

    def to_payload(self) -> dict[str, Any]:
        """Serialize the signed canonical payload for durable storage."""

        return {**self._unsigned_payload(), "payload_digest": self.payload_digest}

    def to_job_envelope(
        self,
        *,
        ownership_generation: int,
        available_at: float = 0.0,
    ) -> MessageRoutingJobEnvelope:
        """Build the durable routing job envelope for this payload."""

        key = self.session_key
        return MessageRoutingJobEnvelope(
            job_id=self.routing_job_id,
            idempotency_key=self.idempotency_key,
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=ownership_generation,
            trace_id=self.trace_id,
            correlation_id=self.routing_job_id,
            causation_id=self._platform_event_id(),
            occurred_at=self.observed_at,
            available_at=available_at,
            payload=self.to_payload(),
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> IngressRoutingPayload:
        """Deserialize a payload while verifying exact fields and digest."""

        actual_fields = frozenset(str(key) for key in payload)
        if actual_fields != _INGRESS_PAYLOAD_FIELDS:
            missing = sorted(_INGRESS_PAYLOAD_FIELDS - actual_fields)
            extra = sorted(actual_fields - _INGRESS_PAYLOAD_FIELDS)
            raise IngressRoutingPayloadError(
                f"ingress payload fields differ: missing={missing}, extra={extra}"
            )
        event = payload.get("event")
        if not isinstance(event, Mapping):
            raise IngressRoutingPayloadError("event must be an object")
        instance = cls(
            version=_required_int(payload, "version"),
            event=dict(event),
            adapter_instance_id=_required_text(payload, "adapter_instance_id"),
            adapter_platform=_optional_text(payload, "adapter_platform"),
            message_xml=str(payload.get("message_xml") or ""),
            trace_id=_required_text(payload, "trace_id"),
            observed_at=_required_float(payload, "observed_at"),
            base_session_id=_required_text(payload, "base_session_id"),
            bot_id=_optional_text(payload, "bot_id"),
            bot_binding_id=_optional_text(payload, "bot_binding_id"),
            bot_session_id=_optional_text(payload, "bot_session_id"),
            fresh_at_ingress=_required_bool(payload, "fresh_at_ingress"),
        )
        if _required_text(payload, "payload_digest") != instance.payload_digest:
            raise IngressRoutingPayloadError("payload_digest does not match canonical payload")
        return instance

    def _unsigned_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "event": dict(self.event),
            "adapter_instance_id": self.adapter_instance_id,
            "adapter_platform": self.adapter_platform,
            "message_xml": self.message_xml,
            "trace_id": self.trace_id,
            "observed_at": self.observed_at,
            "base_session_id": self.base_session_id,
            "bot_id": self.bot_id,
            "bot_binding_id": self.bot_binding_id,
            "bot_session_id": self.bot_session_id,
            "fresh_at_ingress": self.fresh_at_ingress,
        }

    def _platform_event_id(self) -> str:
        event = self.to_event()
        if event.message is not None and event.message.id:
            return str(event.message.id)
        return str(event.id or event.sn or "")

    def _identity_payload(self) -> dict[str, Any]:
        return {
            "event": dict(self.event),
            "adapter_instance_id": self.adapter_instance_id,
            "adapter_platform": self.adapter_platform,
            "message_xml": self.message_xml,
            "base_session_id": self.base_session_id,
            "bot_id": self.bot_id,
            "bot_binding_id": self.bot_binding_id,
            "bot_session_id": self.bot_session_id,
        }


@dataclass(slots=True, frozen=True)
class MessageRoutingJobEnvelope:
    """Versioned routing input persisted in the message-log transaction.

    The payload is intentionally owned by core dispatch. Persistence treats it
    as an opaque JSON object and never imports Agent runtime types.
    """

    job_id: str
    idempotency_key: str
    trace_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    profile_id: str = ""
    session_id: str = ""
    ownership_generation: int = 0
    correlation_id: str = ""
    causation_id: str = ""
    occurred_at: float = 0.0
    available_at: float = 0.0
    version: int = MESSAGE_ROUTING_JOB_VERSION

    def __post_init__(self) -> None:
        """Normalize durable identifiers and reject invalid timestamps."""

        if self.version != MESSAGE_ROUTING_JOB_VERSION:
            raise ValueError(f"unsupported message routing job version: {self.version}")
        for field_name in (
            "job_id",
            "idempotency_key",
            "trace_id",
            "profile_id",
            "session_id",
            "correlation_id",
            "causation_id",
        ):
            object.__setattr__(
                self,
                field_name,
                str(getattr(self, field_name) or "").strip(),
            )
        if not self.job_id:
            raise ValueError("job_id must not be empty")
        if not self.idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        if not self.trace_id:
            raise ValueError("trace_id must not be empty")
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", self.trace_id)
        generation = self.ownership_generation
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise ValueError("ownership_generation must be an integer")
        if self.profile_id or self.session_id or generation:
            if not self.profile_id or not self.session_id or generation < 1:
                raise ValueError(
                    "profile_id, session_id, and positive ownership_generation "
                    "must be provided together"
                )
        elif generation != 0:
            raise ValueError("unscoped routing jobs require ownership_generation zero")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        object.__setattr__(self, "payload", _freeze_json(self.payload))
        for field_name in ("occurred_at", "available_at"):
            value = getattr(self, field_name)
            if isinstance(value, bool):
                raise ValueError(f"{field_name} must be a finite non-negative number")
            numeric = float(value)
            if not math.isfinite(numeric) or numeric < 0:
                raise ValueError(f"{field_name} must be a finite non-negative number")
            object.__setattr__(self, field_name, numeric)


def _canonical_json_object(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_json_array(value: list[Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _optional_text(values: Mapping[str, Any], field_name: str) -> str:
    return str(values.get(field_name) or "").strip()


def _required_text(values: Mapping[str, Any], field_name: str) -> str:
    value = _optional_text(values, field_name)
    if not value:
        raise IngressRoutingPayloadError(f"{field_name} must not be empty")
    return value


def _required_int(values: Mapping[str, Any], field_name: str) -> int:
    value = values.get(field_name)
    if isinstance(value, bool):
        raise IngressRoutingPayloadError(f"{field_name} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise IngressRoutingPayloadError(f"{field_name} must be an integer") from exc


def _required_float(values: Mapping[str, Any], field_name: str) -> float:
    value = values.get(field_name)
    if isinstance(value, bool):
        raise IngressRoutingPayloadError(f"{field_name} must be a number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise IngressRoutingPayloadError(f"{field_name} must be a number") from exc
    if not math.isfinite(result):
        raise IngressRoutingPayloadError(f"{field_name} must be finite")
    return result


def _required_bool(values: Mapping[str, Any], field_name: str) -> bool:
    value = values.get(field_name)
    if not isinstance(value, bool):
        raise IngressRoutingPayloadError(f"{field_name} must be a boolean")
    return value


__all__ = [
    "AGENT_ROUTE_MAILBOX_KIND",
    "AGENT_ROUTE_MAILBOX_SOURCE",
    "AGENT_ROUTE_OUTBOX_VERSION",
    "AgentRouteOutboxStatus",
    "INGRESS_ROUTING_PAYLOAD_VERSION",
    "IngressRoutingPayload",
    "IngressRoutingPayloadError",
    "MESSAGE_ROUTING_JOB_VERSION",
    "MessageRoutingJobEnvelope",
    "MessageRoutingJobStatus",
]
