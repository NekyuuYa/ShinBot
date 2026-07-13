"""Pure contracts for actor-owned externally visible chat actions.

Model-facing workflows produce :class:`ExternalActionIntent` values.  Only an
accepted actor transition may bind an intent to an operation and turn it into
an :class:`ExternalActionRequest` effect.  Neither contract performs I/O.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectExecutionContract,
    EffectLane,
)
from shinbot.agent.runtime.session_actor.events import SessionEffect
from shinbot.core.dispatch.agent_identity import SessionKey

_ACTION_ID_NAMESPACE = uuid.UUID("cf8135f0-9ef3-57c5-9c7d-1bf27afe2231")
EXTERNAL_ACTION_COMPLETION_EVENT_KIND = "ExternalActionCompleted"
LEGACY_EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION = 1
EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION = 2
_RESERVED_MODEL_FIELDS = frozenset(
    {
        "claim_id",
        "contract_signature",
        "contract_version",
        "effect_id",
        "idempotency_key",
        "operation_id",
        "ownership_generation",
        "profile_id",
        "session_id",
    }
)


class _FrozenDict(dict[str, Any]):
    """JSON-compatible mapping that rejects in-place mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable external action mappings are immutable")

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
        raise TypeError("durable external action sequences are immutable")

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


class ExternalActionKind(StrEnum):
    """Externally visible action kinds accepted from Agent workflows."""

    SEND_REPLY = "send_reply"
    SEND_POKE = "send_poke"
    SEND_REACTION = "send_reaction"


class ExternalActionReceiptStatus(StrEnum):
    """Durable action receipt outcomes and recoverable intermediate states."""

    PREPARED = "prepared"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    REJECTED_BEFORE_DISPATCH = "rejected_before_dispatch"
    ABANDONED_BEFORE_DISPATCH = "abandoned_before_dispatch"
    UNKNOWN = "unknown"

    @property
    def automatic_retry_allowed(self) -> bool:
        """Return whether retry cannot duplicate a possibly accepted action."""

        return self in {
            ExternalActionReceiptStatus.PREPARED,
            ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH,
        }

    @property
    def terminal(self) -> bool:
        """Return whether automatic execution must stop for this receipt."""

        return self in {
            ExternalActionReceiptStatus.SUCCEEDED,
            ExternalActionReceiptStatus.ABANDONED_BEFORE_DISPATCH,
            ExternalActionReceiptStatus.UNKNOWN,
        }


_LEGACY_EXTERNAL_ACTION_EFFECT_CONTRACTS = tuple(
    EffectExecutionContract(
        effect_kind=kind.value,
        version=LEGACY_EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION,
        lane=EffectLane.DEFAULT,
        completion_event_kind=EXTERNAL_ACTION_COMPLETION_EVENT_KIND,
        timeout_seconds=30.0,
        max_attempts=5,
        retry_base_seconds=1.0,
        retry_max_seconds=30.0,
        priority=20,
        # v1 effects may already be durable. Keep their pre-declaration
        # signature while resolving their outcomes through the legacy baseline.
        outcome_fence_fields=None,
    )
    for kind in ExternalActionKind
)
_CURRENT_EXTERNAL_ACTION_EFFECT_CONTRACTS = tuple(
    EffectExecutionContract(
        effect_kind=kind.value,
        version=EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION,
        lane=EffectLane.DEFAULT,
        completion_event_kind=EXTERNAL_ACTION_COMPLETION_EVENT_KIND,
        timeout_seconds=30.0,
        max_attempts=5,
        retry_base_seconds=1.0,
        retry_max_seconds=30.0,
        priority=20,
        outcome_fence_fields=("action_ordinal", "request_digest"),
    )
    for kind in ExternalActionKind
)
_EXTERNAL_ACTION_EFFECT_CONTRACTS = (
    *_LEGACY_EXTERNAL_ACTION_EFFECT_CONTRACTS,
    *_CURRENT_EXTERNAL_ACTION_EFFECT_CONTRACTS,
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ExternalActionIntent:
    """Normalized model-selected action without runtime-controlled fences."""

    kind: ExternalActionKind
    tool_call_id: str
    action_ordinal: int
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate identity and deeply freeze the proposed action payload."""

        try:
            kind = ExternalActionKind(self.kind)
        except ValueError as exc:
            raise ValueError(f"unsupported external action kind: {self.kind!r}") from exc
        object.__setattr__(self, "kind", kind)
        tool_call_id = _required_text(self.tool_call_id, field_name="tool_call_id")
        object.__setattr__(self, "tool_call_id", tool_call_id)
        if isinstance(self.action_ordinal, bool) or not isinstance(
            self.action_ordinal,
            int,
        ):
            raise TypeError("action_ordinal must be a non-negative integer")
        if self.action_ordinal < 0:
            raise ValueError("action_ordinal must be a non-negative integer")
        payload = _freeze_json_object(self.payload, field_name="action payload")
        reserved = sorted(_RESERVED_MODEL_FIELDS.intersection(payload))
        if reserved:
            raise ValueError(
                "model-selected action payload contains runtime-reserved fields: "
                + ", ".join(reserved)
            )
        object.__setattr__(self, "payload", payload)

    @property
    def canonical_payload_json(self) -> str:
        """Return the canonical action arguments used for durable identity."""

        return _canonical_json(self.payload)


@dataclass(slots=True, frozen=True, kw_only=True)
class ExternalActionRequest:
    """Actor-bound external action ready to become a durable effect.

    ``key.session_id`` identifies the bot-scoped actor.  ``target_session_id``
    is separately copied from ingress as the adapter/base transport session;
    it is deliberately included in the canonical request digest instead of
    being reconstructed from actor identity.
    """

    key: SessionKey
    ownership_generation: int
    operation_id: str
    source_event_id: str
    instance_id: str
    target_session_id: str
    intent: ExternalActionIntent
    contract_version: int = EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION

    def __post_init__(self) -> None:
        """Validate runtime-controlled provenance and action contract version."""

        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        _require_positive_int(self.contract_version, field_name="contract_version")
        for field_name in (
            "operation_id",
            "source_event_id",
            "instance_id",
            "target_session_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        if not self.target_session_id.startswith(f"{self.instance_id}:"):
            raise ValueError(
                "target_session_id must be a base transport session for instance_id"
            )
        if not isinstance(self.intent, ExternalActionIntent):
            raise TypeError("intent must be ExternalActionIntent")

    @property
    def request_digest(self) -> str:
        """Return a digest of the exact external request, excluding provenance."""

        return hashlib.sha256(
            _canonical_json(
                {
                    "contract_version": self.contract_version,
                    "instance_id": self.instance_id,
                    "kind": self.intent.kind.value,
                    "payload": _thaw_json(self.intent.payload),
                    "target_session_id": self.target_session_id,
                }
            ).encode("utf-8")
        ).hexdigest()

    @property
    def effect_id(self) -> str:
        """Return the deterministic effect identity for the accepted intent."""

        identity = _canonical_json(
            {
                "action_ordinal": self.intent.action_ordinal,
                "kind": self.intent.kind.value,
                "operation_id": self.operation_id,
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "tool_call_id": self.intent.tool_call_id,
            }
        )
        return f"external-action:{uuid.uuid5(_ACTION_ID_NAMESPACE, identity).hex}"

    @property
    def idempotency_key(self) -> str:
        """Return the runtime-owned external idempotency key."""

        return self.effect_id.replace(
            "external-action:",
            "external-action-idempotency:",
            1,
        )

    def to_effect_payload(self) -> dict[str, Any]:
        """Return plain JSON data suitable for a durable action effect."""

        return {
            "action_ordinal": self.intent.action_ordinal,
            "instance_id": self.instance_id,
            "operation_id": self.operation_id,
            "payload": _thaw_json(self.intent.payload),
            "request_digest": self.request_digest,
            "source_event_id": self.source_event_id,
            "target_session_id": self.target_session_id,
            "tool_call_id": self.intent.tool_call_id,
        }


def builtin_external_action_effect_contracts(
) -> tuple[EffectExecutionContract, ...]:
    """Return the versioned execution contracts for visible action effects."""

    return _EXTERNAL_ACTION_EFFECT_CONTRACTS


def builtin_external_action_effect_contract(
    kind: ExternalActionKind | str,
    *,
    version: int | None = None,
) -> EffectExecutionContract:
    """Resolve one visible-action contract by exact or current identity."""

    try:
        normalized_kind = ExternalActionKind(kind)
    except (TypeError, ValueError) as exc:
        raise KeyError(f"unknown external action effect kind: {kind!r}") from exc
    matching = tuple(
        contract
        for contract in _EXTERNAL_ACTION_EFFECT_CONTRACTS
        if contract.effect_kind == normalized_kind.value
        and (version is None or contract.version == version)
    )
    if matching:
        return max(matching, key=lambda contract: contract.version)
    requested = "current" if version is None else f"v{version}"
    raise KeyError(
        "unknown external action effect contract: "
        f"{normalized_kind.value!r} {requested}"
    )


def materialize_external_action_effect(
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_id: str,
    source_event_id: str,
    instance_id: str,
    target_session_id: str,
    intent: ExternalActionIntent,
) -> SessionEffect:
    """Bind one accepted model intent to a deterministic durable effect.

    The actor supplies every provenance field. The logical effect slot depends
    only on the operation, model tool-call id, action ordinal, action kind, and
    stable session key. Exact request content remains in the payload digest so
    reusing a slot with different normalized arguments fails closed in the
    receipt store.
    """

    request = ExternalActionRequest(
        key=key,
        ownership_generation=ownership_generation,
        operation_id=operation_id,
        source_event_id=source_event_id,
        instance_id=instance_id,
        target_session_id=target_session_id,
        intent=intent,
        contract_version=EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION,
    )
    contract = builtin_external_action_effect_contract(request.intent.kind)
    return SessionEffect(
        effect_id=request.effect_id,
        kind=request.intent.kind.value,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload=request.to_effect_payload(),
        idempotency_key=request.idempotency_key,
        operation_id=request.operation_id,
    )


def materialize_external_action_effects(
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_id: str,
    source_event_id: str,
    instance_id: str,
    target_session_id: str,
    intents: Iterable[ExternalActionIntent],
) -> tuple[SessionEffect, ...]:
    """Materialize accepted intents in supplied workflow order.

    This pure projection does not promise effect execution order. The runtime
    must add per-operation serialization before registering concurrent action
    handlers.
    """

    return tuple(
        materialize_external_action_effect(
            key=key,
            ownership_generation=ownership_generation,
            operation_id=operation_id,
            source_event_id=source_event_id,
            instance_id=instance_id,
            target_session_id=target_session_id,
            intent=intent,
        )
        for intent in intents
    )


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_positive_int(value: object, *, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _freeze_json_object(value: object, *, field_name: str) -> _FrozenDict:
    frozen = _freeze_json(value, path=field_name)
    if not isinstance(frozen, _FrozenDict):
        raise TypeError(f"{field_name} must be a mapping")
    return frozen


def _freeze_json(value: object, *, path: str) -> Any:
    if isinstance(value, Mapping):
        items: list[tuple[str, Any]] = []
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
    raise TypeError(f"{path} must contain only JSON-compatible values")


def _thaw_json(value: object) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


__all__ = [
    "EXTERNAL_ACTION_COMPLETION_EVENT_KIND",
    "EXTERNAL_ACTION_EFFECT_CONTRACT_VERSION",
    "ExternalActionIntent",
    "ExternalActionKind",
    "ExternalActionReceiptStatus",
    "ExternalActionRequest",
    "builtin_external_action_effect_contract",
    "builtin_external_action_effect_contracts",
    "materialize_external_action_effect",
    "materialize_external_action_effects",
]
