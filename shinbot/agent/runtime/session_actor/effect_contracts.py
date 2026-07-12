"""Versioned execution contracts for durable session-actor effects."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from functools import lru_cache
from types import MappingProxyType

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
}


_LEGACY_V1_OUTCOME_FENCE_FIELDS: dict[str, tuple[str, ...]] = {
    effect_kind: tuple(sorted({*DEFAULT_OUTCOME_FENCE_FIELDS, *fields}))
    for effect_kind, fields in _CURRENT_OUTCOME_FENCE_FIELDS.items()
}
"""Compatibility projections for v1 control effects with stricter reducers."""


class EffectLane(StrEnum):
    """Independently supervised effect execution lanes."""

    CONTROL = "control"
    PLANNER = "planner"
    DEFAULT = "default"
    ORPHAN = "orphan"


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

        effect_kind = str(self.effect_kind or "").strip()
        event_kind = str(self.completion_event_kind or "").strip()
        source = str(self.completion_source or "").strip()
        if not effect_kind:
            raise ValueError("effect contract kind must not be empty")
        if not event_kind:
            raise ValueError("completion event kind must not be empty")
        if not source:
            raise ValueError("completion source must not be empty")
        if self.version < 1:
            raise ValueError("effect contract version must be at least one")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("effect timeout_seconds must be finite and positive")
        if self.max_attempts < 1:
            raise ValueError("effect max_attempts must be at least one")
        if not math.isfinite(self.retry_base_seconds) or self.retry_base_seconds < 0:
            raise ValueError("effect retry_base_seconds must be finite and non-negative")
        if (
            not math.isfinite(self.retry_max_seconds)
            or self.retry_max_seconds < self.retry_base_seconds
        ):
            raise ValueError(
                "effect retry_max_seconds must be finite and at least retry_base_seconds"
            )
        if self.priority < 0:
            raise ValueError("effect priority must not be negative")
        outcome_fence_fields = self.outcome_fence_fields
        if outcome_fence_fields is not None:
            if isinstance(outcome_fence_fields, str):
                raise TypeError("outcome_fence_fields must be a tuple of field names")
            normalized_fence_fields = tuple(
                str(field_name or "").strip() for field_name in outcome_fence_fields
            )
            if not all(normalized_fence_fields):
                raise ValueError("outcome_fence_fields must not contain empty names")
            if len(set(normalized_fence_fields)) != len(normalized_fence_fields):
                raise ValueError("outcome_fence_fields must not contain duplicates")
            outcome_fence_fields = tuple(sorted(normalized_fence_fields))
        object.__setattr__(self, "effect_kind", effect_kind)
        object.__setattr__(self, "completion_event_kind", event_kind)
        object.__setattr__(self, "completion_source", source)
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
            previous = by_ref.get(contract.ref)
            if previous is not None and previous != contract:
                raise ValueError(
                    "effect contract authority received conflicting policies for "
                    f"{contract.effect_kind}:v{contract.version}"
                )
            by_ref[contract.ref] = contract
        self._contracts = MappingProxyType(dict(by_ref))

    def contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the sealed policies in stable durable identity order."""

        return tuple(
            sorted(
                self._contracts.values(),
                key=lambda contract: (contract.effect_kind, contract.version),
            )
        )

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

        normalized_kind = str(effect_kind or "").strip()
        normalized_signature = str(signature or "").strip()
        if not normalized_kind:
            raise EffectContractAuthorityError("effect contract kind must not be empty")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise EffectContractAuthorityError("effect contract version is invalid")
        if not normalized_signature:
            raise EffectContractAuthorityError(
                "effect contract signature must not be empty"
            )
        contract = self._contracts.get((normalized_kind, version))
        if contract is None:
            raise EffectContractAuthorityError(
                "effect contract is not registered by the sealed authority: "
                f"{normalized_kind}:v{version}"
            )
        if contract.signature != normalized_signature:
            raise EffectContractAuthorityError(
                "effect contract signature does not match the sealed authority: "
                f"{normalized_kind}:v{version}"
            )
        return contract


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
    "EffectContractAuthority",
    "EffectContractAuthorityError",
    "EffectExecutionContract",
    "EffectLane",
    "DEFAULT_OUTCOME_FENCE_FIELDS",
    "builtin_effect_contract",
    "builtin_effect_contract_authority",
    "builtin_session_actor_effect_contracts",
    "resolved_outcome_fence_fields",
]
