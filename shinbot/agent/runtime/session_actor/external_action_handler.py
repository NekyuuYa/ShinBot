"""Effect-handler boundary for durable externally visible Agent actions.

The handler is deliberately adapter-agnostic. Runtime activation supplies a
small dispatch port after it has selected Actor v2 ownership; this module owns
the receipt protocol and makes it impossible for terminal or order-blocked
results to reach that port.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.external_action_store import (
    ClaimedExternalAction,
    ExternalActionOrderBlockedResult,
    ExternalActionReceipt,
    ExternalActionTerminalResult,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionRequest,
    builtin_external_action_effect_contract,
    builtin_external_action_effect_contracts,
)
from shinbot.persistence.records import MessageLogRecord


class ExternalActionHandlerError(RuntimeError):
    """Base error raised before an external-action effect can complete."""


class ExternalActionRetryRequired(ExternalActionHandlerError):
    """Signal a safe no-dispatch retry through the generic effect executor."""


class ExternalActionPreDispatchRejected(ExternalActionHandlerError):
    """A dispatch port proved failure before it could invoke the platform."""

    def __init__(
        self,
        reason_code: str,
        *,
        reason_message: str = "",
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        """Capture durable rejection evidence without executing an action."""

        self.reason_code = _required_text(reason_code, field_name="reason_code")
        self.reason_message = str(reason_message or "")
        self.evidence = dict(evidence or {})
        super().__init__(self.reason_code)


@dataclass(slots=True, frozen=True)
class ExternalActionDispatchResult:
    """Evidence returned only after a dispatch port completed platform I/O."""

    platform_result: Mapping[str, Any]
    assistant_message: MessageLogRecord | None = None

    def __post_init__(self) -> None:
        """Detach caller-owned result mappings before receipt settlement."""

        if not isinstance(self.platform_result, Mapping):
            raise TypeError("platform_result must be a mapping")
        object.__setattr__(self, "platform_result", dict(self.platform_result))
        if self.assistant_message is not None and not isinstance(
            self.assistant_message,
            MessageLogRecord,
        ):
            raise TypeError("assistant_message must be MessageLogRecord or None")


class ExternalActionDispatchPort(Protocol):
    """Platform boundary invoked only after a durable action claim exists."""

    async def dispatch(
        self,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        """Perform exactly one externally visible action for a live receipt claim."""


class ExternalActionReceiptPort(Protocol):
    """Receipt persistence operations required by the effect handler."""

    async def prepare(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
    ) -> ExternalActionReceipt:
        """Persist or validate the action receipt before dispatch."""

    async def begin_execution(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
    ) -> (
        ClaimedExternalAction
        | ExternalActionOrderBlockedResult
        | ExternalActionTerminalResult
        | None
    ):
        """Claim the receipt only when adapter dispatch is allowed."""

    async def reject_before_dispatch(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: Mapping[str, Any] | None = None,
    ) -> ExternalActionReceipt:
        """Persist a retryable, proven pre-dispatch rejection."""

    async def mark_unknown(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: Mapping[str, Any] | None = None,
    ) -> ExternalActionReceipt:
        """Persist an ambiguous outcome after dispatch may have begun."""

    async def settle_succeeded(
        self,
        claim: ClaimedExternalAction,
        *,
        platform_result: Mapping[str, Any],
        assistant_message: MessageLogRecord | None = None,
    ) -> ExternalActionReceipt:
        """Persist success and any assistant message atomically."""


class ExternalActionEffectHandler:
    """Execute a visible action only through a receipt-fenced dispatch port."""

    def __init__(
        self,
        *,
        receipts: ExternalActionReceiptPort,
        dispatcher: ExternalActionDispatchPort,
    ) -> None:
        """Bind persistence and platform boundaries without runtime globals."""

        self._receipts = receipts
        self._dispatcher = dispatcher

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Prepare, claim, and dispatch one external action effect.

        Only :class:`ClaimedExternalAction` crosses into ``dispatcher``.
        Terminal receipts complete the outer effect without I/O. Order-blocked
        and already-executing receipts raise a retry signal so the generic
        executor releases the outer effect instead of treating the action as
        delivered.
        """

        request = external_action_request_from_effect(context)
        await self._receipts.prepare(request, effect_claim=context.claim)
        execution = await self._receipts.begin_execution(
            request,
            effect_claim=context.claim,
        )
        if isinstance(execution, ClaimedExternalAction):
            return await self._dispatch_claimed(request, execution)
        if isinstance(execution, ExternalActionTerminalResult):
            return _completion_for_receipt(
                execution.receipt,
                disposition=execution.reason_code,
            )
        if isinstance(execution, ExternalActionOrderBlockedResult):
            raise ExternalActionRetryRequired(
                "external action is blocked by " + execution.reason_code
            )
        if execution is None:
            raise ExternalActionRetryRequired(
                "external action already has a live execution claim"
            )
        raise TypeError("receipt port returned an unsupported action execution result")

    async def _dispatch_claimed(
        self,
        request: ExternalActionRequest,
        action_claim: ClaimedExternalAction,
    ) -> EffectHandlerResult:
        """Invoke the platform port once and settle the resulting receipt."""

        try:
            result = await self._dispatcher.dispatch(request, action_claim)
        except ExternalActionPreDispatchRejected as exc:
            await self._receipts.reject_before_dispatch(
                action_claim,
                reason_code=exc.reason_code,
                reason_message=exc.reason_message,
                evidence=exc.evidence,
            )
            raise ExternalActionRetryRequired(
                "external action rejected before dispatch: " + exc.reason_code
            ) from exc
        except asyncio.CancelledError:
            await self._mark_cancelled_unknown(action_claim)
            raise
        except Exception as exc:
            receipt = await self._mark_unknown_after_dispatch(
                action_claim,
                reason_code="dispatch_exception",
                reason_message=type(exc).__name__,
            )
            return _completion_for_receipt(
                receipt,
                disposition="dispatch_exception",
            )
        if not isinstance(result, ExternalActionDispatchResult):
            receipt = await self._mark_unknown_after_dispatch(
                action_claim,
                reason_code="dispatch_result_invalid",
                reason_message="dispatcher returned an invalid result type",
            )
            return _completion_for_receipt(
                receipt,
                disposition="dispatch_result_invalid",
            )
        try:
            receipt = await self._receipts.settle_succeeded(
                action_claim,
                platform_result=result.platform_result,
                assistant_message=result.assistant_message,
            )
        except Exception as exc:
            receipt = await self._mark_unknown_after_dispatch(
                action_claim,
                reason_code="success_settlement_failed",
                reason_message=type(exc).__name__,
            )
            return _completion_for_receipt(
                receipt,
                disposition="success_settlement_failed",
            )
        return _completion_for_receipt(receipt, disposition="dispatched")

    async def _mark_unknown_after_dispatch(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str,
    ) -> ExternalActionReceipt:
        """Persist ambiguity whenever adapter execution may already have run."""

        return await self._receipts.mark_unknown(
            claim,
            reason_code=reason_code,
            reason_message=reason_message,
        )

    async def _mark_cancelled_unknown(self, claim: ClaimedExternalAction) -> None:
        """Record ambiguity before propagating cancellation to the executor."""

        try:
            await self._receipts.mark_unknown(
                claim,
                reason_code="dispatch_cancelled",
                reason_message="effect handler cancelled while dispatch may be active",
            )
        except Exception:
            # The executing lease remains durable and recovery will turn it
            # unknown. Never let secondary evidence failure authorize a retry.
            return


def register_external_action_effect_handlers(
    registry: EffectHandlerRegistry,
    *,
    receipts: ExternalActionReceiptPort,
    dispatcher: ExternalActionDispatchPort,
) -> ExternalActionEffectHandler:
    """Register one receipt-fenced handler for every external action contract.

    Runtime activation owns when this registration becomes live. Keeping the
    function explicit prevents legacy traffic from accidentally enabling Actor
    v2 platform dispatch while still giving activation one complete contract
    registration point.
    """

    handler = ExternalActionEffectHandler(receipts=receipts, dispatcher=dispatcher)
    for contract in builtin_external_action_effect_contracts():
        registry.register(
            contract.effect_kind,
            handler,
            contract=contract,
        )
    return handler


def external_action_request_from_effect(
    context: EffectExecutionContext,
) -> ExternalActionRequest:
    """Decode and validate a runtime-owned action request from one effect."""

    effect = context.effect
    payload = _required_mapping(effect.payload, field_name="effect.payload")
    try:
        kind = ExternalActionKind(effect.kind)
    except ValueError as exc:
        raise ExternalActionHandlerError(
            f"unsupported external action effect kind: {effect.kind!r}"
        ) from exc
    action_payload = _required_mapping(payload.get("payload"), field_name="payload")
    intent = ExternalActionIntent(
        kind=kind,
        tool_call_id=_required_text(
            payload.get("tool_call_id"),
            field_name="tool_call_id",
        ),
        action_ordinal=_nonnegative_int(
            payload.get("action_ordinal"),
            field_name="action_ordinal",
        ),
        payload=dict(action_payload),
    )
    operation_id = _required_text(
        payload.get("operation_id"),
        field_name="operation_id",
    )
    if operation_id != effect.operation_id:
        raise ExternalActionHandlerError("effect payload operation_id changed identity")
    source_event_id = _required_text(
        payload.get("source_event_id"),
        field_name="source_event_id",
    )
    if source_event_id != effect.source_event_id:
        raise ExternalActionHandlerError("effect payload source_event_id changed identity")
    target_session_id = _required_text(
        payload.get("target_session_id"),
        field_name="target_session_id",
    )
    request = ExternalActionRequest(
        key=effect.key,
        ownership_generation=effect.ownership_generation,
        operation_id=operation_id,
        source_event_id=source_event_id,
        instance_id=_required_text(
            payload.get("instance_id"),
            field_name="instance_id",
        ),
        target_session_id=target_session_id,
        intent=intent,
        contract_version=effect.contract_version,
    )
    contract = builtin_external_action_effect_contract(
        kind,
        version=effect.contract_version,
    )
    if effect.contract_signature != contract.signature:
        raise ExternalActionHandlerError(
            "external action effect contract signature changed identity"
        )
    if request.effect_id != effect.effect_id:
        raise ExternalActionHandlerError("external action effect_id changed identity")
    if request.idempotency_key != effect.idempotency_key:
        raise ExternalActionHandlerError(
            "external action idempotency_key changed identity"
        )
    if _required_text(
        payload.get("request_digest"),
        field_name="request_digest",
    ) != request.request_digest:
        raise ExternalActionHandlerError(
            "external action request_digest changed identity"
        )
    if effect.kind != request.intent.kind.value:
        raise ExternalActionHandlerError("external action kind changed identity")
    return request


def _completion_for_receipt(
    receipt: ExternalActionReceipt,
    *,
    disposition: str,
) -> EffectHandlerResult:
    """Build the only completion payload accepted after receipt settlement."""

    return EffectHandlerResult(
        payload={
            "action_ordinal": receipt.action_ordinal,
            "dispatch_disposition": _required_text(
                disposition,
                field_name="disposition",
            ),
            "receipt_idempotency_key": receipt.idempotency_key,
            "receipt_status": receipt.status.value,
            "request_digest": receipt.request_digest,
        }
    )


def _required_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ExternalActionHandlerError(f"{field_name} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ExternalActionHandlerError(f"{field_name} keys must be strings")
    return value


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ExternalActionHandlerError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ExternalActionHandlerError(f"{field_name} must not be empty")
    return normalized


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ExternalActionHandlerError(
            f"{field_name} must be a non-negative integer"
        )
    return value


__all__ = [
    "ExternalActionDispatchPort",
    "ExternalActionDispatchResult",
    "ExternalActionEffectHandler",
    "ExternalActionHandlerError",
    "ExternalActionPreDispatchRejected",
    "ExternalActionReceiptPort",
    "ExternalActionRetryRequired",
    "external_action_request_from_effect",
    "register_external_action_effect_handlers",
]
