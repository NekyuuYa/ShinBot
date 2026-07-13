"""Unit coverage for the receipt-fenced external action effect handler."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    EffectExecutionContext,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.external_action_handler import (
    ExternalActionDispatchResult,
    ExternalActionEffectHandler,
    ExternalActionPreDispatchRejected,
    ExternalActionRetryRequired,
    external_action_request_from_effect,
    register_external_action_effect_handlers,
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
    ExternalActionReceiptStatus,
    ExternalActionRequest,
    builtin_external_action_effect_contract,
    builtin_external_action_effect_contracts,
)
from shinbot.core.dispatch.agent_identity import SessionKey

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


class _EffectStore:
    """Minimal renewal port; handler-only tests never need a lease renewal."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Return the existing claim for the execution-context protocol."""

        return claim


class _Dispatcher:
    """Controlled platform boundary used to prove handler dispatch behavior."""

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.calls: list[tuple[ExternalActionRequest, ClaimedExternalAction]] = []

    async def dispatch(
        self,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        """Return the configured result after recording one attempted dispatch."""

        self.calls.append((request, claim))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome  # type: ignore[return-value]


class _Receipts:
    """In-memory receipt port exposing each handler-side state transition."""

    def __init__(self, execution: object, request: ExternalActionRequest) -> None:
        self.execution = execution
        self.request = request
        self.prepare_calls = 0
        self.reject_calls = 0
        self.unknown_calls = 0
        self.success_calls = 0

    async def prepare(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
    ) -> ExternalActionReceipt:
        """Record preparation without changing the configured execution result."""

        assert request == self.request
        self.prepare_calls += 1
        return _receipt(request, ExternalActionReceiptStatus.PREPARED)

    async def begin_execution(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
    ) -> object:
        """Return the test's exact fenced execution outcome."""

        assert request == self.request
        return self.execution

    async def reject_before_dispatch(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> ExternalActionReceipt:
        """Persist a safe pre-dispatch rejection for retry tests."""

        del reason_code, reason_message, evidence
        self.reject_calls += 1
        return replace(
            claim.receipt,
            status=ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH,
        )

    async def mark_unknown(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> ExternalActionReceipt:
        """Persist an ambiguous outcome after a dispatcher boundary is crossed."""

        del reason_code, reason_message, evidence
        self.unknown_calls += 1
        return replace(claim.receipt, status=ExternalActionReceiptStatus.UNKNOWN)

    async def settle_succeeded(
        self,
        claim: ClaimedExternalAction,
        *,
        platform_result: dict[str, Any],
        assistant_message: object | None = None,
    ) -> ExternalActionReceipt:
        """Persist success after the controlled dispatcher returns evidence."""

        del platform_result, assistant_message
        self.success_calls += 1
        return replace(claim.receipt, status=ExternalActionReceiptStatus.SUCCEEDED)


def _request(*, action_ordinal: int = 0) -> ExternalActionRequest:
    return ExternalActionRequest(
        key=_KEY,
        ownership_generation=1,
        operation_id="round-a",
        source_event_id="round-completed-a",
        instance_id="instance-a",
        target_session_id="instance-a:group:room-a",
        intent=ExternalActionIntent(
            kind=ExternalActionKind.SEND_REPLY,
            tool_call_id=f"tool-{action_ordinal}",
            action_ordinal=action_ordinal,
            payload={"text": "hello"},
        ),
    )


def _context(request: ExternalActionRequest) -> EffectExecutionContext:
    contract = builtin_external_action_effect_contract(
        request.intent.kind,
        version=request.contract_version,
    )
    envelope = DurableEffectEnvelope(
        effect_id=request.effect_id,
        key=request.key,
        kind=request.intent.kind.value,
        idempotency_key=request.idempotency_key,
        ownership_generation=request.ownership_generation,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload=request.to_effect_payload(),
        source_event_id=request.source_event_id,
        operation_id=request.operation_id,
    )
    claim = ClaimedEffect(
        claim_id="effect-claim-a",
        effect=envelope,
        worker_id="effect-worker-a",
        attempt_count=1,
        claimed_at=10.0,
        lease_expires_at=20.0,
    )
    return EffectExecutionContext(_EffectStore(), claim)


def test_external_action_handler_registry_covers_v1_and_v2() -> None:
    request = _request()
    receipts = _Receipts(_claimed_action(request), request)
    dispatcher = _Dispatcher(
        ExternalActionDispatchResult(platform_result={"message_id": "message-a"})
    )
    registry = EffectHandlerRegistry()

    handler = register_external_action_effect_handlers(
        registry,
        receipts=receipts,
        dispatcher=dispatcher,
    )

    for contract in builtin_external_action_effect_contracts():
        resolved_contract, resolved_handler = registry.resolve(
            contract.effect_kind,
            contract.version,
        )
        assert resolved_contract is contract
        assert resolved_handler is handler


def test_external_action_handler_decodes_persisted_v1_contract() -> None:
    request = replace(_request(), contract_version=1)

    decoded = external_action_request_from_effect(_context(request))

    assert decoded == request


def _receipt(
    request: ExternalActionRequest,
    status: ExternalActionReceiptStatus,
    *,
    claim_id: str = "",
    lease_owner: str = "",
) -> ExternalActionReceipt:
    return ExternalActionReceipt(
        receipt_seq=1,
        idempotency_key=request.idempotency_key,
        effect_id=request.effect_id,
        operation_id=request.operation_id,
        action_ordinal=request.intent.action_ordinal,
        key=request.key,
        ownership_generation=request.ownership_generation,
        kind=request.intent.kind,
        contract_version=request.contract_version,
        request_digest=request.request_digest,
        request_json=json.dumps(
            request.to_effect_payload(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        status=status,
        attempt_count=1 if status is not ExternalActionReceiptStatus.PREPARED else 0,
        claim_id=claim_id,
        lease_owner=lease_owner,
        lease_until=20.0 if claim_id else None,
        prepared_at=1.0,
        execution_started_at=10.0 if claim_id else None,
        updated_at=10.0,
    )


def _claimed_action(
    request: ExternalActionRequest,
) -> ClaimedExternalAction:
    receipt = _receipt(
        request,
        ExternalActionReceiptStatus.EXECUTING,
        claim_id="effect-claim-a",
        lease_owner="effect-worker-a",
    )
    return ClaimedExternalAction(
        receipt=receipt,
        claim_id="effect-claim-a",
        worker_id="effect-worker-a",
        attempt_count=1,
        claimed_at=10.0,
        lease_expires_at=20.0,
    )


@pytest.mark.asyncio
async def test_terminal_receipt_completes_without_calling_dispatcher() -> None:
    request = _request()
    terminal = ExternalActionTerminalResult(
        receipt=_receipt(request, ExternalActionReceiptStatus.UNKNOWN),
        reason_code="execution_lease_expired",
    )
    receipts = _Receipts(terminal, request)
    dispatcher = _Dispatcher(ExternalActionDispatchResult(platform_result={}))

    result = await ExternalActionEffectHandler(
        receipts=receipts,
        dispatcher=dispatcher,
    )(_context(request))

    assert receipts.prepare_calls == 1
    assert dispatcher.calls == []
    assert result.payload["receipt_status"] == "unknown"
    assert result.payload["request_digest"] == request.request_digest


@pytest.mark.asyncio
async def test_order_blocked_receipt_never_reaches_dispatcher() -> None:
    request = _request(action_ordinal=1)
    blocked = ExternalActionOrderBlockedResult(
        receipt=_receipt(request, ExternalActionReceiptStatus.PREPARED),
        predecessor=None,
        reason_code="predecessor_not_succeeded",
    )
    receipts = _Receipts(blocked, request)
    dispatcher = _Dispatcher(ExternalActionDispatchResult(platform_result={}))

    with pytest.raises(ExternalActionRetryRequired, match="predecessor_not_succeeded"):
        await ExternalActionEffectHandler(
            receipts=receipts,
            dispatcher=dispatcher,
        )(_context(request))

    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_claimed_dispatch_settles_success_and_returns_receipt_provenance() -> None:
    request = _request()
    receipts = _Receipts(_claimed_action(request), request)
    dispatcher = _Dispatcher(
        ExternalActionDispatchResult(platform_result={"platform_msg_id": "p-1"})
    )

    result = await ExternalActionEffectHandler(
        receipts=receipts,
        dispatcher=dispatcher,
    )(_context(request))

    assert len(dispatcher.calls) == 1
    assert receipts.success_calls == 1
    assert receipts.unknown_calls == 0
    assert result.payload["receipt_status"] == "succeeded"
    assert result.payload["action_ordinal"] == 0
    assert result.payload["request_digest"] == request.request_digest


@pytest.mark.asyncio
async def test_invalid_dispatch_result_marks_unknown_instead_of_retrying_io() -> None:
    request = _request()
    receipts = _Receipts(_claimed_action(request), request)
    dispatcher = _Dispatcher({"not": "a dispatch result"})

    result = await ExternalActionEffectHandler(
        receipts=receipts,
        dispatcher=dispatcher,
    )(_context(request))

    assert len(dispatcher.calls) == 1
    assert receipts.unknown_calls == 1
    assert receipts.success_calls == 0
    assert result.payload["receipt_status"] == "unknown"
    assert result.payload["dispatch_disposition"] == "dispatch_result_invalid"


@pytest.mark.asyncio
async def test_pre_dispatch_rejection_releases_outer_effect_for_safe_retry() -> None:
    request = _request()
    receipts = _Receipts(_claimed_action(request), request)
    dispatcher = _Dispatcher(
        ExternalActionPreDispatchRejected("adapter_unavailable")
    )

    with pytest.raises(ExternalActionRetryRequired, match="adapter_unavailable"):
        await ExternalActionEffectHandler(
            receipts=receipts,
            dispatcher=dispatcher,
        )(_context(request))

    assert len(dispatcher.calls) == 1
    assert receipts.reject_calls == 1
    assert receipts.unknown_calls == 0
