"""Tests for strict profile routing in the inactive Actor v2 handler graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.profile_handler_graph import (
    ActorProfileEffectHandlerBundle,
    ActorProfileHandlerGraphError,
    ActorV2ProfileHandlerGraph,
    UnknownActorWorkflowProfile,
)


@dataclass(slots=True)
class _Key:
    """Minimal durable key projection used by the outer handler wrapper."""

    profile_id: str


@dataclass(slots=True)
class _Effect:
    """Minimal effect projection used by the outer handler wrapper."""

    kind: str
    contract_version: int
    contract_signature: str
    key: _Key


@dataclass(slots=True)
class _Context:
    """Minimal handler context used before executor-specific work begins."""

    effect: _Effect


@dataclass(slots=True)
class _RecordingHandler:
    """Profile-local handler that reveals which bundle received an effect."""

    profile_id: str
    calls: list[str] = field(default_factory=list)

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Record the profile selected by the graph and return it as payload."""

        del context
        self.calls.append(self.profile_id)
        return EffectHandlerResult(payload={"profile_id": self.profile_id})


def _context(*, profile_id: str) -> EffectExecutionContext:
    """Build one wrapper-only context for the selected review contract."""

    contract = builtin_effect_contract("run_review_workflow", version=1)
    return cast(
        EffectExecutionContext,
        _Context(
            effect=_Effect(
                kind=contract.effect_kind,
                contract_version=contract.version,
                contract_signature=contract.signature,
                key=_Key(profile_id=profile_id),
            )
        ),
    )


@pytest.mark.asyncio
async def test_profile_handler_graph_routes_each_effect_to_its_exact_profile() -> None:
    """Two configured profiles must never share the default workflow bundle."""

    contract = builtin_effect_contract("run_review_workflow", version=1)
    default_handler = _RecordingHandler("__default__")
    bot_handler = _RecordingHandler("bot-a")
    graph = ActorV2ProfileHandlerGraph(
        effect_contract_authority=builtin_effect_contract_authority(),
        bundles=(
            ActorProfileEffectHandlerBundle(
                profile_id="__default__",
                handlers={contract.ref: default_handler},
            ),
            ActorProfileEffectHandlerBundle(
                profile_id="bot-a",
                handlers={contract.ref: bot_handler},
            ),
        ),
    )
    registry = EffectHandlerRegistry(
        contract_authority=builtin_effect_contract_authority()
    )
    graph.register(registry)
    _, handler = registry.resolve(contract.effect_kind, contract.version)

    default_result = await handler(_context(profile_id="__default__"))
    bot_result = await handler(_context(profile_id="bot-a"))

    assert default_result.payload == {"profile_id": "__default__"}
    assert bot_result.payload == {"profile_id": "bot-a"}
    assert default_handler.calls == ["__default__"]
    assert bot_handler.calls == ["bot-a"]


@pytest.mark.asyncio
async def test_profile_handler_graph_rejects_unknown_profile_without_default_fallback() -> None:
    """A removed or unknown durable profile must fail before any handler runs."""

    contract = builtin_effect_contract("run_review_workflow", version=1)
    handler = _RecordingHandler("__default__")
    graph = ActorV2ProfileHandlerGraph(
        effect_contract_authority=builtin_effect_contract_authority(),
        bundles=(
            ActorProfileEffectHandlerBundle(
                profile_id="__default__",
                handlers={contract.ref: handler},
            ),
        ),
    )
    registry = EffectHandlerRegistry(
        contract_authority=builtin_effect_contract_authority()
    )
    graph.register(registry)
    _, wrapper = registry.resolve(contract.effect_kind, contract.version)

    with pytest.raises(UnknownActorWorkflowProfile, match="missing-profile"):
        await wrapper(_context(profile_id="missing-profile"))

    assert handler.calls == []


def test_profile_handler_graph_requires_identical_contract_coverage() -> None:
    """One outer contract cannot silently route to a partial profile bundle."""

    review_contract = builtin_effect_contract("run_review_workflow", version=1)
    reply_contract = builtin_effect_contract("run_active_reply_workflow", version=1)

    with pytest.raises(ActorProfileHandlerGraphError, match="same durable effect"):
        ActorV2ProfileHandlerGraph(
            effect_contract_authority=builtin_effect_contract_authority(),
            bundles=(
                ActorProfileEffectHandlerBundle(
                    profile_id="__default__",
                    handlers={review_contract.ref: _RecordingHandler("__default__")},
                ),
                ActorProfileEffectHandlerBundle(
                    profile_id="bot-a",
                    handlers={reply_contract.ref: _RecordingHandler("bot-a")},
                ),
            ),
        )
