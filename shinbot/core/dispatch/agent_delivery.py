"""Versioned durable payload contract for Agent route delivery."""

from __future__ import annotations

import json
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from shinbot.core.dispatch.agent_identity import SessionKey, SessionKeyFactory
from shinbot.core.dispatch.agent_signals import (
    AgentMessageSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
)

AGENT_ROUTE_DELIVERY_VERSION = 1
_DELIVERY_ID_NAMESPACE = uuid.UUID("4a5c6be2-a46d-58f7-ab84-640d19609b59")


class AgentRouteDeliveryError(ValueError):
    """Base error for an invalid or non-durable Agent route delivery."""


class MissingAgentMessageLogId(AgentRouteDeliveryError):
    """Raised when a delivery cannot be recovered from the message log."""


@dataclass(slots=True, frozen=True)
class AgentRouteDelivery:
    """Canonical message delivery prepared at the Agent route boundary.

    A compatibility-only instance may carry ``message_log_id=None`` so the
    legacy signal path keeps working for embedded runtimes without a database.
    Such an instance cannot expose durable IDs or serialize to an actor payload.
    """

    session_key: SessionKey
    bot_id: str
    bot_binding_id: str
    base_session_id: str
    bot_session_id: str
    message_log_id: int | None
    sender_id: str
    instance_id: str
    platform: str
    self_id: str
    is_private: bool
    is_mentioned: bool
    is_mention_to_other: bool
    is_reply_to_bot: bool
    is_poke_to_bot: bool
    is_poke_to_other: bool
    already_handled: bool
    is_stopped: bool
    trace_id: str
    observed_at: float
    event_type: str = "message-created"
    route_rule_id: str = ""
    version: int = AGENT_ROUTE_DELIVERY_VERSION

    def __post_init__(self) -> None:
        """Normalize identifiers and validate the versioned payload boundary."""

        if (
            isinstance(self.version, bool)
            or not isinstance(self.version, int)
            or self.version != AGENT_ROUTE_DELIVERY_VERSION
        ):
            raise AgentRouteDeliveryError(
                f"unsupported Agent route delivery version: {self.version}"
            )
        for field_name in (
            "bot_id",
            "bot_binding_id",
            "base_session_id",
            "bot_session_id",
            "sender_id",
            "instance_id",
            "platform",
            "self_id",
            "trace_id",
            "event_type",
            "route_rule_id",
        ):
            object.__setattr__(
                self,
                field_name,
                str(getattr(self, field_name) or "").strip(),
            )
        if not self.base_session_id:
            raise AgentRouteDeliveryError("base_session_id must not be empty")
        for field_name in (
            "is_private",
            "is_mentioned",
            "is_mention_to_other",
            "is_reply_to_bot",
            "is_poke_to_bot",
            "is_poke_to_other",
            "already_handled",
            "is_stopped",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise AgentRouteDeliveryError(f"{field_name} must be a boolean")
        canonical_key = SessionKeyFactory().create(
            bot_config_id=self.bot_id,
            bot_id=self.bot_id,
            bot_session_id=self.bot_session_id,
            base_session_id=self.base_session_id,
        )
        if self.session_key != canonical_key:
            raise AgentRouteDeliveryError(
                "session_key does not match canonical routing identity"
            )
        if self.message_log_id is not None and (
            isinstance(self.message_log_id, bool)
            or not isinstance(self.message_log_id, int)
            or self.message_log_id < 1
        ):
            raise AgentRouteDeliveryError("message_log_id must be a positive integer")
        if self.message_log_id is not None:
            if not self.instance_id:
                raise AgentRouteDeliveryError(
                    "instance_id is required for durable Agent actor delivery"
                )
            if not self.event_type:
                raise AgentRouteDeliveryError(
                    "event_type is required for durable Agent actor delivery"
                )
            if not self.route_rule_id:
                raise AgentRouteDeliveryError(
                    "route_rule_id is required for durable Agent actor delivery"
                )
        if not _is_nonnegative_finite(self.observed_at):
            raise AgentRouteDeliveryError("observed_at must be finite and non-negative")
        object.__setattr__(self, "observed_at", float(self.observed_at))

    @property
    def actor_deliverable(self) -> bool:
        """Return whether this payload has a recoverable message-log identity."""

        return self.message_log_id is not None

    @property
    def delivery_key(self) -> tuple[str, str, int, str]:
        """Return the canonical profile/session/message/rule delivery key."""

        message_log_id = self._require_message_log_id()
        return (
            self.session_key.profile_id,
            self.session_key.session_id,
            message_log_id,
            self.route_rule_id,
        )

    @property
    def delivery_id(self) -> str:
        """Return a deterministic identity for the durable route delivery."""

        return f"agent-route-delivery:v{self.version}:{self._identity_digest()}"

    @property
    def event_id(self) -> str:
        """Return the version-independent logical actor message event id."""

        return f"message-received:{self._actor_event_digest()}"

    @property
    def idempotency_key(self) -> str:
        """Return the deterministic idempotency key for a future routing outbox."""

        return f"agent-route-idempotency:v{self.version}:{self._identity_digest()}"

    @property
    def compatibility_signal_id(self) -> str:
        """Return the unchanged legacy signal identity used during migration."""

        token: int | str = (
            self.message_log_id if self.message_log_id is not None else "missing"
        )
        return f"message-ingress:{self.base_session_id}:{token}"

    def require_actor_delivery(self) -> AgentRouteDelivery:
        """Reject compatibility-only payloads before any actor handoff."""

        self._require_message_log_id()
        return self

    def to_payload(self) -> dict[str, Any]:
        """Serialize the validated durable payload for persistence or replay."""

        self.require_actor_delivery()
        return {
            "version": self.version,
            "delivery_id": self.delivery_id,
            "event_id": self.event_id,
            "idempotency_key": self.idempotency_key,
            "session_key": {
                "profile_id": self.session_key.profile_id,
                "session_id": self.session_key.session_id,
            },
            "bot_id": self.bot_id,
            "bot_binding_id": self.bot_binding_id,
            "base_session_id": self.base_session_id,
            "bot_session_id": self.bot_session_id,
            "message_log_id": self.message_log_id,
            "sender_id": self.sender_id,
            "instance_id": self.instance_id,
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
            "trace_id": self.trace_id,
            "observed_at": self.observed_at,
            "event_type": self.event_type,
            "route_rule_id": self.route_rule_id,
        }

    def to_mailbox_payload(self) -> dict[str, Any]:
        """Serialize rule-independent actor message ledger content.

        Multiple route rules may independently create delivery outbox rows for
        one persisted message. The actor sees one canonical ``MessageReceived``
        event keyed only by profile, session, and message log identity.
        """

        self.require_actor_delivery()
        return {
            "version": self.version,
            "event_id": self.event_id,
            "session_key": {
                "profile_id": self.session_key.profile_id,
                "session_id": self.session_key.session_id,
            },
            "bot_id": self.bot_id,
            "bot_binding_id": self.bot_binding_id,
            "base_session_id": self.base_session_id,
            "bot_session_id": self.bot_session_id,
            "message_log_id": self.message_log_id,
            "sender_id": self.sender_id,
            "instance_id": self.instance_id,
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
            "trace_id": self.trace_id,
            "observed_at": self.observed_at,
            "event_type": self.event_type,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> AgentRouteDelivery:
        """Deserialize and verify a durable delivery payload.

        The canonical key and all deterministic identifiers are recomputed;
        persisted values cannot silently redirect delivery to another profile.
        """

        factory = SessionKeyFactory()
        version = _required_int(payload, "version")
        bot_id = _optional_text(payload, "bot_id")
        bot_session_id = _optional_text(payload, "bot_session_id")
        base_session_id = _required_text(payload, "base_session_id")
        key = factory.create(
            bot_config_id=bot_id,
            bot_id=bot_id,
            bot_session_id=bot_session_id,
            base_session_id=base_session_id,
        )
        raw_key = payload.get("session_key")
        if not isinstance(raw_key, Mapping):
            raise AgentRouteDeliveryError("session_key must be an object")
        persisted_key = (
            _required_text(raw_key, "profile_id"),
            _required_text(raw_key, "session_id"),
        )
        if persisted_key != (key.profile_id, key.session_id):
            raise AgentRouteDeliveryError(
                "persisted session_key does not match canonical routing identity"
            )
        delivery = cls(
            version=version,
            session_key=key,
            bot_id=bot_id,
            bot_binding_id=_optional_text(payload, "bot_binding_id"),
            base_session_id=base_session_id,
            bot_session_id=bot_session_id,
            message_log_id=_required_int(payload, "message_log_id"),
            sender_id=_optional_text(payload, "sender_id"),
            instance_id=_required_text(payload, "instance_id"),
            platform=_optional_text(payload, "platform"),
            self_id=_optional_text(payload, "self_id"),
            is_private=_required_bool(payload, "is_private"),
            is_mentioned=_required_bool(payload, "is_mentioned"),
            is_mention_to_other=_required_bool(payload, "is_mention_to_other"),
            is_reply_to_bot=_required_bool(payload, "is_reply_to_bot"),
            is_poke_to_bot=_required_bool(payload, "is_poke_to_bot"),
            is_poke_to_other=_required_bool(payload, "is_poke_to_other"),
            already_handled=_required_bool(payload, "already_handled"),
            is_stopped=_required_bool(payload, "is_stopped"),
            trace_id=_optional_text(payload, "trace_id"),
            observed_at=_required_float(payload, "observed_at"),
            event_type=_required_text(payload, "event_type"),
            route_rule_id=_optional_text(payload, "route_rule_id"),
        )
        expected_ids = {
            "delivery_id": delivery.delivery_id,
            "event_id": delivery.event_id,
            "idempotency_key": delivery.idempotency_key,
        }
        for field_name, expected in expected_ids.items():
            if _required_text(payload, field_name) != expected:
                raise AgentRouteDeliveryError(
                    f"{field_name} does not match the canonical delivery key"
                )
        return delivery

    def to_agent_signal(self) -> AgentSignal:
        """Convert this payload back to the legacy AgentSignal contract."""

        meta: dict[str, Any] = {
            "event_type": self.event_type,
            "trace_id": self.trace_id,
            "route_rule_id": self.route_rule_id,
            "delivery_version": self.version,
        }
        if self.actor_deliverable:
            meta.update(
                {
                    "delivery_id": self.delivery_id,
                    "event_id": self.event_id,
                    "idempotency_key": self.idempotency_key,
                    "actor_profile_id": self.session_key.profile_id,
                    "actor_session_id": self.session_key.session_id,
                }
            )
        return AgentSignal(
            signal_id=self.compatibility_signal_id,
            kind=AgentSignalKind.MESSAGE,
            source=AgentSignalSource.MESSAGE_INGRESS,
            bot_id=self.bot_id,
            bot_binding_id=self.bot_binding_id,
            bot_session_id=self.bot_session_id,
            session_id=self.base_session_id,
            occurred_at=self.observed_at,
            message=AgentMessageSignal(
                message_log_id=self.message_log_id,
                sender_id=self.sender_id,
                instance_id=self.instance_id,
                platform=self.platform,
                self_id=self.self_id,
                is_private=self.is_private,
                is_mentioned=self.is_mentioned,
                is_mention_to_other=self.is_mention_to_other,
                is_reply_to_bot=self.is_reply_to_bot,
                is_poke_to_bot=self.is_poke_to_bot,
                is_poke_to_other=self.is_poke_to_other,
                already_handled=self.already_handled,
                is_stopped=self.is_stopped,
            ),
            meta=meta,
        )

    def _require_message_log_id(self) -> int:
        if self.message_log_id is None:
            raise MissingAgentMessageLogId(
                "message_log_id is required for durable Agent actor delivery"
            )
        return self.message_log_id

    def _identity_digest(self) -> str:
        identity = json.dumps(
            [self.version, *self.delivery_key],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return uuid.uuid5(_DELIVERY_ID_NAMESPACE, identity).hex

    def _actor_event_digest(self) -> str:
        identity = json.dumps(
            [
                self.session_key.profile_id,
                self.session_key.session_id,
                self._require_message_log_id(),
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return uuid.uuid5(_DELIVERY_ID_NAMESPACE, identity).hex


def _required_text(values: Mapping[str, Any], field_name: str) -> str:
    value = _optional_text(values, field_name)
    if not value:
        raise AgentRouteDeliveryError(f"{field_name} must not be empty")
    return value


def _optional_text(values: Mapping[str, Any], field_name: str) -> str:
    return str(values.get(field_name) or "").strip()


def _required_int(values: Mapping[str, Any], field_name: str) -> int:
    value = values.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AgentRouteDeliveryError(f"{field_name} must be an integer")
    return value


def _required_float(values: Mapping[str, Any], field_name: str) -> float:
    value = values.get(field_name)
    if isinstance(value, bool):
        raise AgentRouteDeliveryError(f"{field_name} must be a number")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise AgentRouteDeliveryError(f"{field_name} must be a number") from exc
    if not math.isfinite(numeric):
        raise AgentRouteDeliveryError(f"{field_name} must be finite")
    return numeric


def _required_bool(values: Mapping[str, Any], field_name: str) -> bool:
    value = values.get(field_name)
    if not isinstance(value, bool):
        raise AgentRouteDeliveryError(f"{field_name} must be a boolean")
    return value


def _is_nonnegative_finite(value: object) -> bool:
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric >= 0.0


__all__ = [
    "AGENT_ROUTE_DELIVERY_VERSION",
    "AgentRouteDelivery",
    "AgentRouteDeliveryError",
    "MissingAgentMessageLogId",
]
