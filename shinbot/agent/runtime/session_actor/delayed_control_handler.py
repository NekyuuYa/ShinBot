"""Pure completion boundary for actor-owned delayed control effects.

The delay itself belongs to the durable effect outbox's ``available_at``
field.  Once a claimed effect is available, this handler does not recreate a
timer or invoke a legacy runtime: it only authorizes the executor to append
the contract-owned mailbox completion.  The reducer remains the sole owner of
whether that completion still matches the current session state.
"""

from __future__ import annotations

from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectLane,
    builtin_effect_contract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)

DELAYED_CONTROL_EFFECT_KINDS: frozenset[str] = frozenset(
    {
        "enqueue_idle_review_planning_deadline",
        "enqueue_active_chat_exit_request",
        "enqueue_active_chat_round_due",
    }
)
"""Actor effects whose durable availability releases a reducer mailbox event."""


class DelayedControlEffectHandlerError(ValueError):
    """Raised when a non-delayed-control effect reaches this boundary."""


class DelayedControlEffectHandler:
    """Complete one supported delayed control effect without external I/O.

    Contract and outbox validation happen before a handler is invoked by the
    executor.  The narrow re-validation here keeps this object safe when it is
    called directly and prevents future registration drift from silently
    turning another control effect into a timer-envelope effect.
    """

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Return an empty completion payload after validating exact identity."""

        effect = context.effect
        if effect.kind not in DELAYED_CONTROL_EFFECT_KINDS:
            raise DelayedControlEffectHandlerError(
                f"unsupported delayed control effect kind: {effect.kind!r}"
            )
        try:
            contract = builtin_effect_contract(
                effect.kind,
                version=effect.contract_version,
            )
        except KeyError as exc:
            raise DelayedControlEffectHandlerError(
                "unsupported delayed control effect contract: "
                f"{effect.kind}:v{effect.contract_version}"
            ) from exc
        if contract.lane is not EffectLane.CONTROL:
            raise DelayedControlEffectHandlerError(
                "delayed control effect must use the control execution lane"
            )
        if effect.contract_signature != contract.signature:
            raise DelayedControlEffectHandlerError(
                "delayed control effect contract signature changed identity"
            )
        return EffectHandlerResult()


def register_delayed_control_effect_handlers(
    registry: EffectHandlerRegistry,
) -> DelayedControlEffectHandler:
    """Register one pure handler for every supported delayed control contract.

    Both legacy and current versions are registered so a future activation can
    recover old durable outbox rows without routing them through a legacy
    scheduler.  Registering handlers never starts an executor or exposes an
    actor wake target.
    """

    handler = DelayedControlEffectHandler()
    for contract in builtin_session_actor_effect_contracts():
        if contract.effect_kind in DELAYED_CONTROL_EFFECT_KINDS:
            registry.register(
                contract.effect_kind,
                handler,
                contract=contract,
            )
    return handler


__all__ = [
    "DELAYED_CONTROL_EFFECT_KINDS",
    "DelayedControlEffectHandler",
    "DelayedControlEffectHandlerError",
    "register_delayed_control_effect_handlers",
]
