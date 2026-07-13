"""Versioned execution contracts for durable session-actor effects."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import IntEnum, StrEnum
from functools import lru_cache
from types import MappingProxyType
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.json_validation import (
    DurableJSONValidationError,
    validate_durable_json,
)

DEFAULT_OUTCOME_FENCE_FIELDS: tuple[str, ...] = (
    "plan_id",
    "active_epoch",
    "activity_generation",
    "expected_active_epoch",
    "expected_activity_generation",
    "expected_state_revision",
    "state_revision",
    "input_watermark",
    "input_ledger_sequence",
    "trigger",
    "source",
)
"""Former global baseline for legacy contracts and direct store callers."""


_CURRENT_OUTCOME_FENCE_FIELDS: dict[str, tuple[str, ...]] = {
    "enqueue_idle_review_planning_deadline": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "deadline_event_id",
        "failure_event_id",
        "source",
        "trigger",
    ),
    "active_chat_runtime_reconciliation": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "desired_state",
        "control_effect_kind",
        "control_effect_id",
        "reconciliation_cycle",
    ),
    "idle_review_planning_cancellation_reconciliation": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "desired_state",
        "control_effect_kind",
        "control_effect_id",
        "reconciliation_cycle",
    ),
    "cancel_idle_review_planning": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "superseded_by_event_id",
    ),
    "stop_active_chat_runtime": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ),
    "cancel_review_workflow": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "superseded_by_event_id",
    ),
    "enqueue_active_chat_exit_request": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "superseded_by_event_id",
        "trigger",
        "expected_active_epoch",
        "expected_message_watermark",
    ),
    "enqueue_active_chat_round_due": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "superseded_by_event_id",
        "schedule_id",
        "schedule_revision",
    ),
    "run_active_reply_workflow": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ),
    "run_active_chat_bootstrap": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ),
    "run_active_chat_round": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ),
    "run_review_workflow": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ),
    "run_idle_review_planning": (
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
        "source",
        "trigger",
    ),
}


_LEGACY_V1_OUTCOME_FENCE_FIELDS: dict[str, tuple[str, ...]] = {
    effect_kind: tuple(sorted({*DEFAULT_OUTCOME_FENCE_FIELDS, *fields}))
    for effect_kind, fields in _CURRENT_OUTCOME_FENCE_FIELDS.items()
    if effect_kind
    in {
        "cancel_review_workflow",
        "enqueue_active_chat_exit_request",
        "enqueue_active_chat_round_due",
    }
}
"""Frozen projections for v1 effects that predate explicit v2 declarations."""

for _external_action_kind in ("send_reply", "send_poke", "send_reaction"):
    _LEGACY_V1_OUTCOME_FENCE_FIELDS[_external_action_kind] = tuple(
        sorted(
            {
                *DEFAULT_OUTCOME_FENCE_FIELDS,
                "action_ordinal",
                "request_digest",
            }
        )
    )


class EffectLane(StrEnum):
    """Independently supervised effect execution lanes."""

    CONTROL = "control"
    PLANNER = "planner"
    DEFAULT = "default"
    ORPHAN = "orphan"


def _canonical_contract_text(value: object, *, field_name: str) -> str:
    if isinstance(value, StrEnum):
        raw_value = value.value
        if type(raw_value) is not str:
            raise TypeError(f"{field_name} StrEnum value must be a str")
        normalized = raw_value.strip()
    elif type(value) is str:
        normalized = value.strip()
    else:
        raise TypeError(f"{field_name} must be a str or StrEnum")
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _canonical_contract_integer(value: object, *, field_name: str) -> int:
    if isinstance(value, IntEnum):
        raw_value = value.value
        if type(raw_value) is not int:
            raise TypeError(f"{field_name} IntEnum value must be an int")
        return int(raw_value)
    if type(value) is int:
        return value
    raise TypeError(f"{field_name} must be an int or IntEnum")


def _canonical_contract_number(value: object, *, field_name: str) -> float:
    if isinstance(value, IntEnum):
        raw_value: int | float = value.value
    elif type(value) is int or type(value) is float:
        raw_value = value
    else:
        raise TypeError(f"{field_name} must be an int, float, or IntEnum")
    try:
        return float(raw_value)
    except OverflowError as exc:
        raise ValueError(f"{field_name} must be finite") from exc


@dataclass(slots=True, frozen=True)
class EffectExecutionContract:
    """Immutable execution policy for one durable effect kind and version."""

    effect_kind: str
    version: int
    lane: EffectLane
    completion_event_kind: str
    completion_source: str = "effect_executor"
    timeout_seconds: float = 30.0
    max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 60.0
    priority: int = 100
    outcome_fence_fields: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        """Validate bounded execution and retry policy."""

        effect_kind = _canonical_contract_text(
            self.effect_kind,
            field_name="effect contract kind",
        )
        event_kind = _canonical_contract_text(
            self.completion_event_kind,
            field_name="completion event kind",
        )
        source = _canonical_contract_text(
            self.completion_source,
            field_name="completion source",
        )
        version = _canonical_contract_integer(
            self.version,
            field_name="effect contract version",
        )
        max_attempts = _canonical_contract_integer(
            self.max_attempts,
            field_name="effect max_attempts",
        )
        priority = _canonical_contract_integer(
            self.priority,
            field_name="effect priority",
        )
        timeout_seconds = _canonical_contract_number(
            self.timeout_seconds,
            field_name="effect timeout_seconds",
        )
        retry_base_seconds = _canonical_contract_number(
            self.retry_base_seconds,
            field_name="effect retry_base_seconds",
        )
        retry_max_seconds = _canonical_contract_number(
            self.retry_max_seconds,
            field_name="effect retry_max_seconds",
        )
        if not isinstance(self.lane, EffectLane):
            raise TypeError("effect lane must be an EffectLane")
        if version < 1:
            raise ValueError("effect contract version must be at least one")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("effect timeout_seconds must be finite and positive")
        if max_attempts < 1:
            raise ValueError("effect max_attempts must be at least one")
        if not math.isfinite(retry_base_seconds) or retry_base_seconds < 0:
            raise ValueError("effect retry_base_seconds must be finite and non-negative")
        if (
            not math.isfinite(retry_max_seconds)
            or retry_max_seconds < retry_base_seconds
        ):
            raise ValueError(
                "effect retry_max_seconds must be finite and at least retry_base_seconds"
            )
        if priority < 0:
            raise ValueError("effect priority must not be negative")
        outcome_fence_fields = self.outcome_fence_fields
        if outcome_fence_fields is not None:
            if type(outcome_fence_fields) is not tuple:
                raise TypeError("outcome_fence_fields must be a tuple of field names")
            normalized_fence_fields = tuple(
                _canonical_contract_text(
                    field_name,
                    field_name="outcome_fence_fields entry",
                )
                for field_name in outcome_fence_fields
            )
            outcome_fence_fields = tuple(sorted(set(normalized_fence_fields)))
        object.__setattr__(self, "effect_kind", effect_kind)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "completion_event_kind", event_kind)
        object.__setattr__(self, "completion_source", source)
        object.__setattr__(self, "timeout_seconds", timeout_seconds)
        object.__setattr__(self, "max_attempts", max_attempts)
        object.__setattr__(self, "retry_base_seconds", retry_base_seconds)
        object.__setattr__(self, "retry_max_seconds", retry_max_seconds)
        object.__setattr__(self, "priority", priority)
        object.__setattr__(self, "outcome_fence_fields", outcome_fence_fields)

    @property
    def ref(self) -> tuple[str, int]:
        """Return the durable lookup identity for this contract."""

        return self.effect_kind, self.version

    @property
    def signature(self) -> str:
        """Return a stable digest of every execution-policy field."""

        policy: dict[str, object] = {
            "completion_event_kind": self.completion_event_kind,
            "completion_source": self.completion_source,
            "effect_kind": self.effect_kind,
            "lane": self.lane.value,
            "max_attempts": self.max_attempts,
            "priority": self.priority,
            "retry_base_seconds": self.retry_base_seconds,
            "retry_max_seconds": self.retry_max_seconds,
            "timeout_seconds": self.timeout_seconds,
            "version": self.version,
        }
        # ``None`` identifies pre-declaration v1 contracts and intentionally
        # preserves their existing durable signature for recovery.
        if self.outcome_fence_fields is not None:
            policy["outcome_fence_fields"] = list(self.outcome_fence_fields)
        canonical = json.dumps(
            policy,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("ascii")).hexdigest()


class DurableEffectDeclaration(Protocol):
    """Storage-agnostic durable effect fields required at commit boundaries."""

    kind: str
    contract_version: int
    contract_signature: str
    payload: Mapping[str, Any]


class EffectDeclarationValidationError(ValueError):
    """Raised when an effect does not match its sealed durable contract."""


_NONNEGATIVE_INTEGER_FENCE_FIELDS = frozenset(
    {
        "action_ordinal",
        "active_epoch",
        "activity_generation",
        "expected_active_epoch",
        "expected_activity_generation",
        "expected_message_watermark",
        "expected_state_revision",
        "input_ledger_sequence",
        "input_watermark",
        "reconciliation_cycle",
        "schedule_revision",
        "state_revision",
    }
)
_NULLABLE_INTEGER_FENCE_FIELDS = frozenset(
    {
        "expected_active_epoch",
        "expected_activity_generation",
        "expected_message_watermark",
        "expected_state_revision",
        "input_ledger_sequence",
    }
)
_TEXT_FENCE_FIELDS = frozenset(
    {
        "completion_event_id",
        "control_effect_id",
        "control_effect_kind",
        "deadline_event_id",
        "desired_state",
        "failure_event_id",
        "plan_id",
        "request_digest",
        "schedule_id",
        "source",
        "superseded_by_event_id",
        "trigger",
    }
)
_NULLABLE_TEXT_FENCE_FIELDS = frozenset({"superseded_by_event_id"})


def resolved_outcome_fence_fields(
    contract: EffectExecutionContract,
) -> tuple[str, ...]:
    """Return the contract's immutable outcome projection declaration.

    ``None`` is reserved for persisted pre-declaration contracts.  Known v1
    control effects retain their former baseline fields plus the event fences
    their historic reducer validators require.  This preserves recovery
    compatibility without changing their stored contract signatures.
    """

    if contract.outcome_fence_fields is not None:
        return contract.outcome_fence_fields
    if contract.version == 1:
        return _LEGACY_V1_OUTCOME_FENCE_FIELDS.get(
            contract.effect_kind,
            DEFAULT_OUTCOME_FENCE_FIELDS,
        )
    return DEFAULT_OUTCOME_FENCE_FIELDS


class EffectContractAuthorityError(RuntimeError):
    """Raised when a durable effect is absent from its sealed policy authority."""


class EffectContractAuthority:
    """Immutable contract lookup used to validate durable effect settlement.

    The effect outbox retains only a contract reference and its signature.  A
    settlement boundary must therefore resolve that reference through a
    process-owned, immutable policy snapshot instead of accepting a field
    projection supplied by an executor or another caller.
    """

    __slots__ = ("_contracts",)

    def __init__(self, contracts: Iterable[EffectExecutionContract]) -> None:
        """Freeze exact contract policies by durable ``(kind, version)`` key.

        Args:
            contracts: Policies allowed to settle effects through this authority.

        Raises:
            TypeError: If a value is not an execution contract.
            ValueError: If a durable contract reference has conflicting policies.
        """

        by_ref: dict[tuple[str, int], EffectExecutionContract] = {}
        for contract in contracts:
            if not isinstance(contract, EffectExecutionContract):
                raise TypeError(
                    "effect contract authority requires EffectExecutionContract values"
                )
            effect_kind, version = contract.ref
            if type(effect_kind) is not str or type(version) is not int:
                raise TypeError("effect contract authority requires canonical references")
            ref = (effect_kind, version)
            previous = by_ref.get(ref)
            if previous is not None and previous != contract:
                raise ValueError(
                    "effect contract authority received conflicting policies for "
                    f"{contract.effect_kind}:v{contract.version}"
                )
            by_ref[ref] = contract
        self._contracts = MappingProxyType(dict(by_ref))

    def contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the sealed policies in stable durable identity order."""

        return tuple(
            sorted(
                self._contracts.values(),
                key=lambda contract: (contract.effect_kind, contract.version),
            )
        )

    @property
    def sealed(self) -> bool:
        """Return whether this immutable authority is sealed for execution."""

        return True

    def resolve(
        self,
        *,
        effect_kind: str,
        version: int,
        signature: str,
    ) -> EffectExecutionContract:
        """Resolve one exact persisted contract reference and signature.

        Args:
            effect_kind: Durable effect kind stored in the outbox.
            version: Durable contract version stored in the outbox.
            signature: Durable policy signature stored in the outbox.

        Raises:
            EffectContractAuthorityError: If the reference is unknown or the
                persisted signature does not match the sealed policy.
        """

        try:
            normalized_kind = _canonical_contract_text(
                effect_kind,
                field_name="effect contract kind",
            )
            normalized_version = _canonical_contract_integer(
                version,
                field_name="effect contract version",
            )
            normalized_signature = _canonical_contract_text(
                signature,
                field_name="effect contract signature",
            )
        except (TypeError, ValueError) as exc:
            raise EffectContractAuthorityError(str(exc)) from exc
        if normalized_version < 1:
            raise EffectContractAuthorityError("effect contract version is invalid")
        contract = self._contracts.get((normalized_kind, normalized_version))
        if contract is None:
            raise EffectContractAuthorityError(
                "effect contract is not registered by the sealed authority: "
                f"{normalized_kind}:v{normalized_version}"
            )
        if contract.signature != normalized_signature:
            raise EffectContractAuthorityError(
                "effect contract signature does not match the sealed authority: "
                f"{normalized_kind}:v{normalized_version}"
            )
        return contract


def validate_effect_declaration(
    effect: DurableEffectDeclaration,
    *,
    authority: EffectContractAuthority,
) -> EffectExecutionContract:
    """Validate one effect against an explicitly supplied sealed authority.

    Version-one contracts predate outcome-fence declarations. Their exact
    durable reference and signature remain valid, but this validator does not
    retroactively require fields that historical rows may not contain. Every
    declared field is mandatory for version two and later.

    Args:
        effect: Effect declaration about to enter an actor transition or store.
        authority: Exact immutable contract graph shared with effect execution.

    Returns:
        The exact contract resolved from the supplied authority.

    Raises:
        EffectDeclarationValidationError: If identity, JSON, or declared fence
            fields are malformed or do not match the sealed authority.
        TypeError: If ``authority`` is not an immutable contract authority.
    """

    if not isinstance(authority, EffectContractAuthority):
        raise TypeError("authority must be an EffectContractAuthority")
    if not authority.sealed:
        raise TypeError("effect contract authority must be sealed")
    effect_kind = _exact_nonempty_text(effect.kind, field_name="effect.kind")
    contract_version = _positive_json_integer(
        effect.contract_version,
        field_name="effect.contract_version",
    )
    contract_signature = _exact_nonempty_text(
        effect.contract_signature,
        field_name="effect.contract_signature",
    )
    payload = effect.payload
    if not isinstance(payload, Mapping):
        raise EffectDeclarationValidationError("effect.payload must be a JSON object")
    try:
        validate_durable_json(payload, path="effect.payload")
    except DurableJSONValidationError as exc:
        raise EffectDeclarationValidationError(str(exc)) from exc
    try:
        contract = authority.resolve(
            effect_kind=effect_kind,
            version=contract_version,
            signature=contract_signature,
        )
    except EffectContractAuthorityError as exc:
        raise EffectDeclarationValidationError(str(exc)) from exc
    declared_fields = contract.outcome_fence_fields
    if declared_fields is None:
        return contract
    missing = tuple(field for field in declared_fields if field not in payload)
    if missing:
        raise EffectDeclarationValidationError(
            "effect payload is missing declared outcome fence fields for "
            f"{effect_kind}: {', '.join(missing)}"
        )
    for field_name in declared_fields:
        _validate_declared_fence_value(field_name, payload[field_name])
    return contract


def _validate_declared_fence_value(field_name: str, value: object) -> None:
    if field_name in _NONNEGATIVE_INTEGER_FENCE_FIELDS:
        if value is None and field_name in _NULLABLE_INTEGER_FENCE_FIELDS:
            return
        _nonnegative_json_integer(
            value,
            field_name=f"effect.payload.{field_name}",
        )
        return
    if field_name in _TEXT_FENCE_FIELDS:
        if value is None and field_name in _NULLABLE_TEXT_FENCE_FIELDS:
            return
        _exact_text(value, field_name=f"effect.payload.{field_name}")


def _exact_nonempty_text(value: object, *, field_name: str) -> str:
    normalized = _exact_text(value, field_name=field_name)
    if not normalized:
        raise EffectDeclarationValidationError(f"{field_name} must not be empty")
    if normalized != normalized.strip():
        raise EffectDeclarationValidationError(
            f"{field_name} must not contain surrounding whitespace"
        )
    return normalized


def _exact_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise EffectDeclarationValidationError(f"{field_name} must be a JSON string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise EffectDeclarationValidationError(
            f"{field_name} must contain valid UTF-8 text"
        ) from exc
    return value


def _positive_json_integer(value: object, *, field_name: str) -> int:
    result = _nonnegative_json_integer(value, field_name=field_name)
    if result < 1:
        raise EffectDeclarationValidationError(
            f"{field_name} must be a positive JSON integer"
        )
    return result


def _nonnegative_json_integer(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EffectDeclarationValidationError(
            f"{field_name} must be a non-negative JSON integer"
        )
    return value


@lru_cache(maxsize=1)
def builtin_effect_contract_authority() -> EffectContractAuthority:
    """Return the sealed authority for the complete Actor v2 contract graph.

    The external-action import is intentionally lazy because that module
    defines its contracts using :class:`EffectExecutionContract`.
    """

    from shinbot.agent.runtime.session_actor.external_actions import (
        builtin_external_action_effect_contracts,
    )

    return EffectContractAuthority(
        (
            *builtin_session_actor_effect_contracts(),
            *builtin_external_action_effect_contracts(),
        )
    )


def builtin_session_actor_effect_contracts() -> tuple[EffectExecutionContract, ...]:
    """Return legacy and current contracts for actor-owned effects."""

    legacy_contracts = tuple(
        replace(contract, outcome_fence_fields=None)
        for contract in (
            EffectExecutionContract(
                effect_kind="enqueue_idle_review_planning_deadline",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="IdleReviewPlanningDeadlineReached",
                timeout_seconds=2.0,
                max_attempts=3,
                retry_base_seconds=0.1,
                retry_max_seconds=1.0,
                priority=0,
            ),
            EffectExecutionContract(
                effect_kind="active_chat_runtime_reconciliation",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="ActiveChatRuntimeReconciled",
                timeout_seconds=10.0,
                max_attempts=8,
                retry_base_seconds=1.0,
                retry_max_seconds=60.0,
                priority=1,
            ),
            EffectExecutionContract(
                effect_kind="idle_review_planning_cancellation_reconciliation",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="IdleReviewPlanningCancellationReconciled",
                timeout_seconds=10.0,
                max_attempts=8,
                retry_base_seconds=1.0,
                retry_max_seconds=60.0,
                priority=1,
            ),
            EffectExecutionContract(
                effect_kind="cancel_idle_review_planning",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="IdleReviewPlanningCancellationCompleted",
                timeout_seconds=5.0,
                max_attempts=5,
                retry_base_seconds=0.5,
                retry_max_seconds=10.0,
                priority=2,
            ),
            EffectExecutionContract(
                effect_kind="stop_active_chat_runtime",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="ActiveChatRuntimeStopped",
                timeout_seconds=5.0,
                max_attempts=5,
                retry_base_seconds=0.5,
                retry_max_seconds=10.0,
                priority=2,
            ),
            EffectExecutionContract(
                effect_kind="cancel_review_workflow",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="ReviewCancellationCompleted",
                timeout_seconds=5.0,
                max_attempts=5,
                retry_base_seconds=0.5,
                retry_max_seconds=10.0,
                priority=2,
            ),
            EffectExecutionContract(
                effect_kind="enqueue_active_chat_exit_request",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="ExitRequested",
                timeout_seconds=2.0,
                max_attempts=3,
                retry_base_seconds=0.1,
                retry_max_seconds=1.0,
                priority=0,
            ),
            EffectExecutionContract(
                effect_kind="enqueue_active_chat_round_due",
                version=1,
                lane=EffectLane.CONTROL,
                completion_event_kind="ActiveChatRoundDue",
                timeout_seconds=2.0,
                max_attempts=3,
                retry_base_seconds=0.1,
                retry_max_seconds=1.0,
                priority=1,
            ),
            EffectExecutionContract(
                effect_kind="run_active_reply_workflow",
                version=1,
                lane=EffectLane.PLANNER,
                completion_event_kind="ActiveReplyCompleted",
                timeout_seconds=60.0,
                max_attempts=3,
                retry_base_seconds=1.0,
                retry_max_seconds=10.0,
                priority=8,
            ),
            EffectExecutionContract(
                effect_kind="run_active_chat_bootstrap",
                version=1,
                lane=EffectLane.PLANNER,
                completion_event_kind="ActiveChatBootstrapCompleted",
                timeout_seconds=45.0,
                max_attempts=3,
                retry_base_seconds=1.0,
                retry_max_seconds=10.0,
                priority=8,
            ),
            EffectExecutionContract(
                effect_kind="run_active_chat_round",
                version=1,
                lane=EffectLane.PLANNER,
                completion_event_kind="ActiveChatRoundCompleted",
                timeout_seconds=60.0,
                max_attempts=3,
                retry_base_seconds=1.0,
                retry_max_seconds=10.0,
                priority=8,
            ),
            EffectExecutionContract(
                effect_kind="run_review_workflow",
                version=1,
                lane=EffectLane.PLANNER,
                completion_event_kind="ReviewCompleted",
                timeout_seconds=180.0,
                max_attempts=3,
                retry_base_seconds=1.0,
                retry_max_seconds=15.0,
                priority=9,
            ),
            EffectExecutionContract(
                effect_kind="run_idle_review_planning",
                version=1,
                lane=EffectLane.PLANNER,
                completion_event_kind="IdleReviewPlanningCompleted",
                timeout_seconds=30.0,
                max_attempts=3,
                retry_base_seconds=1.0,
                retry_max_seconds=10.0,
                priority=10,
            ),
        )
    )
    current_contracts = tuple(
        replace(
            contract,
            version=2,
            outcome_fence_fields=outcome_fence_fields,
        )
        for contract in legacy_contracts
        if (outcome_fence_fields := _CURRENT_OUTCOME_FENCE_FIELDS.get(contract.effect_kind))
        is not None
    )
    return (*legacy_contracts, *current_contracts)


def builtin_effect_contract(
    effect_kind: str,
    *,
    version: int | None = None,
) -> EffectExecutionContract:
    """Resolve one built-in contract by exact or current durable identity."""

    normalized = str(effect_kind or "").strip()
    matching = tuple(
        contract
        for contract in builtin_session_actor_effect_contracts()
        if contract.effect_kind == normalized and (version is None or contract.version == version)
    )
    if not matching:
        requested = "current" if version is None else f"v{version}"
        raise KeyError(f"unknown built-in effect contract: {normalized!r} {requested}")
    return max(matching, key=lambda contract: contract.version)


__all__ = [
    "DurableEffectDeclaration",
    "EffectContractAuthority",
    "EffectContractAuthorityError",
    "EffectDeclarationValidationError",
    "EffectExecutionContract",
    "EffectLane",
    "DEFAULT_OUTCOME_FENCE_FIELDS",
    "builtin_effect_contract",
    "builtin_effect_contract_authority",
    "builtin_session_actor_effect_contracts",
    "resolved_outcome_fence_fields",
    "validate_effect_declaration",
]
