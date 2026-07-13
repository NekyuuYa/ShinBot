"""Unit coverage for pure Actor v2 delayed-control effect completions."""

from __future__ import annotations

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.delayed_control_handler import (
    DELAYED_CONTROL_EFFECT_KINDS,
    DelayedControlEffectHandler,
    DelayedControlEffectHandlerError,
    register_delayed_control_effect_handlers,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    EffectExecutionContext,
    EffectHandlerRegistry,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")
_SUPPORTED_CONTRACTS = tuple(
    contract
    for contract in builtin_session_actor_effect_contracts()
    if contract.effect_kind in DELAYED_CONTROL_EFFECT_KINDS
)


class _EffectStore:
    """Minimal renewal port; these handler tests never renew a lease."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Return the current claim if a test unexpectedly renews it."""

        return claim


def _context(
    effect_kind: str,
    *,
    version: int = 2,
    contract_signature: str | None = None,
) -> EffectExecutionContext:
    """Build one direct handler context with a real built-in contract."""

    contract = builtin_effect_contract(effect_kind, version=version)
    effect = DurableEffectEnvelope(
        effect_id=f"{effect_kind}:effect-a",
        key=_KEY,
        kind=effect_kind,
        idempotency_key=f"{effect_kind}:effect-a",
        ownership_generation=1,
        contract_version=contract.version,
        contract_signature=contract_signature or contract.signature,
        payload={},
        source_event_id="source-event-a",
    )
    return EffectExecutionContext(
        _EffectStore(),
        ClaimedEffect(
            claim_id=f"claim:{effect_kind}",
            effect=effect,
            worker_id="unit-worker",
            attempt_count=1,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "contract",
    _SUPPORTED_CONTRACTS,
    ids=lambda contract: f"{contract.effect_kind}:v{contract.version}",
)
async def test_handler_accepts_every_supported_contract_version(
    contract,
) -> None:
    """Legacy and current durable timer rows complete with no handler payload."""

    result = await DelayedControlEffectHandler()(
        _context(contract.effect_kind, version=contract.version)
    )

    assert result.payload == {}


def test_registration_covers_exactly_supported_delayed_control_contracts() -> None:
    """Registration binds both v1 and v2 without claiming unrelated controls."""

    registry = EffectHandlerRegistry()
    handler = register_delayed_control_effect_handlers(registry)

    assert _SUPPORTED_CONTRACTS
    assert registry.handled_contracts() == tuple(
        sorted(
            _SUPPORTED_CONTRACTS,
            key=lambda contract: (contract.effect_kind, contract.version),
        )
    )
    for contract in _SUPPORTED_CONTRACTS:
        registered, registered_handler = registry.resolve(
            contract.effect_kind,
            contract.version,
        )
        assert registered == contract
        assert registered_handler is handler


@pytest.mark.asyncio
async def test_handler_rejects_an_unrelated_effect_even_when_called_directly() -> None:
    """A future registry mistake cannot turn workflow execution into a timer."""

    with pytest.raises(
        DelayedControlEffectHandlerError,
        match="unsupported delayed control effect kind",
    ):
        await DelayedControlEffectHandler()(
            _context("run_idle_review_planning")
        )


@pytest.mark.asyncio
async def test_handler_rejects_a_changed_contract_signature() -> None:
    """The direct boundary preserves the sealed-contract identity check."""

    with pytest.raises(
        DelayedControlEffectHandlerError,
        match="contract signature changed identity",
    ):
        await DelayedControlEffectHandler()(
            _context(
                "enqueue_idle_review_planning_deadline",
                contract_signature="0" * 64,
            )
        )
