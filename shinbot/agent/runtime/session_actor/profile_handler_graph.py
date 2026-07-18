"""Strict profile routing for inactive Actor v2 effect handlers.

One durable effect registry serves every configured bot profile in a database
domain.  Its handler is therefore never allowed to use the legacy runtime's
default-profile fallback: effects carry a durable ``SessionKey.profile_id`` and
must execute only through the matching frozen profile bundle.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from shinbot.agent.runtime.session_actor.delayed_control_handler import (
    register_delayed_control_effect_handlers,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectExecutionContract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandler,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.execution_control import (
    ReviewCancellationControlPort,
)
from shinbot.agent.runtime.session_actor.execution_control_handler import (
    register_review_cancellation_control_effect_handler,
)
from shinbot.agent.runtime.session_actor.external_action_handler import (
    ExternalActionDispatchPort,
    ExternalActionReceiptPort,
    register_external_action_effect_handlers,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    IdleReviewPlanningWorkflowPort,
    register_idle_review_planning_effect_handler,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    ModelExecutionCancellationControlPort,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_handler import (
    register_model_execution_cancellation_control_effect_handler,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveChatBootstrapWorkflowPort,
    ActiveChatRoundWorkflowPort,
    ActiveReplyWorkflowPort,
    ActorWorkflowLedgerPort,
    ReviewWorkflowPort,
    register_actor_active_chat_workflow_effect_handlers,
    register_actor_workflow_effect_handlers,
)

type EffectHandlerRef = tuple[str, int]


class ActorProfileHandlerGraphError(RuntimeError):
    """Raised when profile-scoped effect composition is not coherent."""


class UnknownActorWorkflowProfile(ActorProfileHandlerGraphError):
    """Raised when a durable effect refers to an unconfigured actor profile."""


@dataclass(slots=True, frozen=True)
class ActorProfileWorkflowPorts:
    """The pure workflow ports owned by one durable actor profile.

    Every field is already profile-local when this value is built.  The graph
    only chooses a bundle; it never falls back to another profile or reaches
    into the legacy scheduler/coordinator.
    """

    profile_id: str
    active_reply_workflow: ActiveReplyWorkflowPort
    review_workflow: ReviewWorkflowPort
    active_chat_bootstrap_workflow: ActiveChatBootstrapWorkflowPort
    active_chat_round_workflow: ActiveChatRoundWorkflowPort
    idle_review_planning_workflow: IdleReviewPlanningWorkflowPort

    def __post_init__(self) -> None:
        """Require one canonical durable profile identity."""

        normalized = _profile_id(self.profile_id)
        object.__setattr__(self, "profile_id", normalized)


@dataclass(slots=True, frozen=True)
class ActorProfileEffectHandlerBundle:
    """Exact handler map for one profile in an inactive actor graph."""

    profile_id: str
    handlers: Mapping[EffectHandlerRef, EffectHandler]

    def __post_init__(self) -> None:
        """Freeze profile-local handler references before they enter the graph."""

        normalized_profile_id = _profile_id(self.profile_id)
        normalized_handlers: dict[EffectHandlerRef, EffectHandler] = {}
        for raw_ref, handler in self.handlers.items():
            ref = _handler_ref(raw_ref)
            if not callable(handler):
                raise TypeError(
                    "actor profile effect handler must be an async-callable object"
                )
            normalized_handlers[ref] = handler
        if not normalized_handlers:
            raise ValueError("actor profile effect handler bundle must not be empty")
        object.__setattr__(self, "profile_id", normalized_profile_id)
        object.__setattr__(
            self,
            "handlers",
            MappingProxyType(dict(normalized_handlers)),
        )


class ProfileAwareEffectHandler:
    """Delegate one exact contract through a strict durable-profile lookup."""

    def __init__(
        self,
        *,
        contract: EffectExecutionContract,
        bundles: Mapping[str, ActorProfileEffectHandlerBundle],
    ) -> None:
        """Bind a single immutable contract to a frozen profile bundle set."""

        self._contract = contract
        self._bundles = bundles

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Run only the handler selected by the effect's durable profile id."""

        effect = context.effect
        if (
            effect.kind != self._contract.effect_kind
            or effect.contract_version != self._contract.version
            or effect.contract_signature != self._contract.signature
        ):
            raise ActorProfileHandlerGraphError(
                "profile-aware handler received a different effect contract"
            )
        profile_id = _profile_id(effect.key.profile_id)
        try:
            bundle = self._bundles[profile_id]
        except KeyError as exc:
            raise UnknownActorWorkflowProfile(
                "no actor workflow bundle exists for durable profile " + profile_id
            ) from exc
        if bundle.profile_id != profile_id:
            raise ActorProfileHandlerGraphError(
                "actor workflow bundle profile identity changed"
            )
        try:
            handler = bundle.handlers[self._contract.ref]
        except KeyError as exc:
            raise ActorProfileHandlerGraphError(
                "actor workflow bundle lacks contract "
                f"{self._contract.effect_kind}:v{self._contract.version}"
            ) from exc
        return await handler(context)


class ActorV2ProfileHandlerGraph:
    """One outer handler graph that routes all supported effects by profile."""

    def __init__(
        self,
        *,
        effect_contract_authority: EffectContractAuthority,
        bundles: Iterable[ActorProfileEffectHandlerBundle],
    ) -> None:
        """Validate identical supported handler coverage across all profiles."""

        if not isinstance(effect_contract_authority, EffectContractAuthority):
            raise TypeError("effect_contract_authority must be an EffectContractAuthority")
        bundles_by_profile: dict[str, ActorProfileEffectHandlerBundle] = {}
        for bundle in bundles:
            if not isinstance(bundle, ActorProfileEffectHandlerBundle):
                raise TypeError(
                    "actor profile handler graph requires ActorProfileEffectHandlerBundle values"
                )
            if bundle.profile_id in bundles_by_profile:
                raise ValueError(
                    "actor profile handler graph received duplicate profile "
                    + bundle.profile_id
                )
            bundles_by_profile[bundle.profile_id] = bundle
        if not bundles_by_profile:
            raise ValueError("actor profile handler graph requires at least one profile")

        expected_refs: frozenset[EffectHandlerRef] | None = None
        for bundle in bundles_by_profile.values():
            refs = frozenset(bundle.handlers)
            if expected_refs is None:
                expected_refs = refs
            elif refs != expected_refs:
                raise ActorProfileHandlerGraphError(
                    "actor profiles must bind the same durable effect contracts"
                )
        assert expected_refs is not None
        authority_contracts = {
            contract.ref: contract for contract in effect_contract_authority.contracts()
        }
        missing_contracts = sorted(expected_refs.difference(authority_contracts))
        if missing_contracts:
            rendered = ", ".join(
                f"{kind}:v{version}" for kind, version in missing_contracts
            )
            raise ActorProfileHandlerGraphError(
                "actor profile handlers reference contracts outside authority: " + rendered
            )

        self._bundles = MappingProxyType(dict(bundles_by_profile))
        self._contracts = tuple(
            sorted(
                (authority_contracts[ref] for ref in expected_refs),
                key=lambda contract: (contract.effect_kind, contract.version),
            )
        )

    @classmethod
    def compose(
        cls,
        *,
        effect_contract_authority: EffectContractAuthority,
        ledger: ActorWorkflowLedgerPort,
        profiles: Iterable[ActorProfileWorkflowPorts],
        external_action_receipts: ExternalActionReceiptPort,
        external_action_dispatcher: ExternalActionDispatchPort,
        review_cancellation_control: ReviewCancellationControlPort,
        model_execution_cancellation_control: ModelExecutionCancellationControlPort | None = None,
    ) -> ActorV2ProfileHandlerGraph:
        """Create complete profile bundles from existing actor-native ports.

        Temporary registries are only a reuse mechanism for the established
        contract registration functions.  They never execute work, start a
        worker, or expose a wake target.
        """

        bundles: list[ActorProfileEffectHandlerBundle] = []
        for profile in profiles:
            if not isinstance(profile, ActorProfileWorkflowPorts):
                raise TypeError(
                    "profiles must contain ActorProfileWorkflowPorts values"
                )
            local_registry = EffectHandlerRegistry(
                contract_authority=effect_contract_authority
            )
            register_actor_workflow_effect_handlers(
                local_registry,
                ledger=ledger,
                active_reply_workflow=profile.active_reply_workflow,
                review_workflow=profile.review_workflow,
            )
            register_actor_active_chat_workflow_effect_handlers(
                local_registry,
                ledger=ledger,
                active_chat_bootstrap_workflow=profile.active_chat_bootstrap_workflow,
                active_chat_round_workflow=profile.active_chat_round_workflow,
            )
            register_idle_review_planning_effect_handler(
                local_registry,
                workflow=profile.idle_review_planning_workflow,
            )
            register_delayed_control_effect_handlers(local_registry)
            register_review_cancellation_control_effect_handler(
                local_registry,
                control=review_cancellation_control,
            )
            if model_execution_cancellation_control is not None:
                register_model_execution_cancellation_control_effect_handler(
                    local_registry,
                    control=model_execution_cancellation_control,
                )
            register_external_action_effect_handlers(
                local_registry,
                receipts=external_action_receipts,
                dispatcher=external_action_dispatcher,
            )
            handlers = {
                contract.ref: local_registry.resolve(*contract.ref)[1]
                for contract in local_registry.handled_contracts()
            }
            bundles.append(
                ActorProfileEffectHandlerBundle(
                    profile_id=profile.profile_id,
                    handlers=handlers,
                )
            )
        return cls(
            effect_contract_authority=effect_contract_authority,
            bundles=bundles,
        )

    @property
    def profile_ids(self) -> tuple[str, ...]:
        """Return frozen durable profile ids in deterministic order."""

        return tuple(sorted(self._bundles))

    @property
    def supported_contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the exact handler-bound contract subset, never all authority."""

        return self._contracts

    def register(self, registry: EffectHandlerRegistry) -> None:
        """Register one strict outer wrapper for every supported contract."""

        if not isinstance(registry, EffectHandlerRegistry):
            raise TypeError("registry must be an EffectHandlerRegistry")
        for contract in self._contracts:
            registry.register(
                contract.effect_kind,
                ProfileAwareEffectHandler(contract=contract, bundles=self._bundles),
                contract=contract,
            )


def _profile_id(value: object) -> str:
    """Return a canonical durable profile id without a fallback path."""

    if not isinstance(value, str):
        raise TypeError("actor profile id must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError("actor profile id must not be empty")
    return normalized


def _handler_ref(value: object) -> EffectHandlerRef:
    """Validate one exact ``(effect_kind, version)`` handler identity."""

    if not isinstance(value, tuple) or len(value) != 2:
        raise TypeError("actor effect handler reference must be a (kind, version) tuple")
    kind, version = value
    if not isinstance(kind, str) or not kind.strip() or kind != kind.strip():
        raise ValueError("actor effect handler kind must be canonical non-empty text")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("actor effect handler version must be a positive integer")
    return kind, version


__all__ = [
    "ActorProfileEffectHandlerBundle",
    "ActorProfileHandlerGraphError",
    "ActorProfileWorkflowPorts",
    "ActorV2ProfileHandlerGraph",
    "EffectHandlerRef",
    "ProfileAwareEffectHandler",
    "UnknownActorWorkflowProfile",
]
