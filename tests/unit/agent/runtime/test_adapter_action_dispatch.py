"""Coverage for adapter transport dispatch beneath the receipt boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.adapter_action_dispatch import (
    AdapterExternalActionDispatcher,
)
from shinbot.agent.runtime.session_actor.external_action_handler import (
    ExternalActionPreDispatchRejected,
)
from shinbot.agent.runtime.session_actor.external_action_store import (
    ClaimedExternalAction,
    ExternalActionReceipt,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionReceiptStatus,
    ExternalActionRequest,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.schema.elements import MessageElement

_KEY = SessionKey("profile-a", "bot-config-a:group:room-a")
_TARGET = "instance-a:group:room-a"


class _Adapter(BaseAdapter):
    """Recording adapter with no network behavior."""

    def __init__(self) -> None:
        super().__init__("instance-a", "test")
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def start(self) -> None:
        """Satisfy the adapter contract for the test double."""

    async def shutdown(self) -> None:
        """Satisfy the adapter contract for the test double."""

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        """Record one visible text dispatch."""

        self.sent.append((target_session, elements))
        return MessageHandle("platform-reply-1", adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Record one non-message platform action."""

        self.calls.append((method, dict(params)))
        return {"ok": True}

    async def get_capabilities(self) -> dict[str, Any]:
        """Satisfy the adapter contract for the test double."""

        return {}


@dataclass(slots=True)
class _Adapters:
    adapter: BaseAdapter | None
    connected: bool = True

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        """Return the controlled adapter only for its stable id."""

        return self.adapter if instance_id == "instance-a" else None

    def is_connected(self, instance_id: str) -> bool:
        """Return the configured connectivity state for the adapter id."""

        return instance_id == "instance-a" and self.connected


@dataclass(slots=True)
class _MessageLogs:
    records: dict[int, dict[str, Any]] = field(default_factory=dict)

    def get(self, message_log_id: int) -> dict[str, Any] | None:
        """Return a detached record matching the database repository contract."""

        record = self.records.get(message_log_id)
        return dict(record) if record is not None else None


@dataclass(slots=True)
class _Database:
    message_logs: _MessageLogs


def _request(
    kind: ExternalActionKind,
    payload: dict[str, Any],
) -> ExternalActionRequest:
    return ExternalActionRequest(
        key=_KEY,
        ownership_generation=1,
        operation_id="round-a",
        source_event_id="round-completed-a",
        instance_id="instance-a",
        target_session_id=_TARGET,
        intent=ExternalActionIntent(
            kind=kind,
            tool_call_id="tool-a",
            action_ordinal=0,
            payload=payload,
        ),
    )


def _claim(request: ExternalActionRequest) -> ClaimedExternalAction:
    receipt = ExternalActionReceipt(
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
        request_json="{}",
        status=ExternalActionReceiptStatus.EXECUTING,
        attempt_count=1,
        claim_id="claim-a",
        lease_owner="worker-a",
        lease_until=20.0,
        prepared_at=1.0,
        execution_started_at=10.0,
        updated_at=10.0,
    )
    return ClaimedExternalAction(
        receipt=receipt,
        claim_id="claim-a",
        worker_id="worker-a",
        attempt_count=1,
        claimed_at=10.0,
        lease_expires_at=20.0,
    )


@pytest.mark.asyncio
async def test_reply_uses_base_transport_session_and_returns_atomic_log_record() -> None:
    adapter = _Adapter()
    dispatcher = AdapterExternalActionDispatcher(
        adapters=_Adapters(adapter),
        database=_Database(_MessageLogs()),
        clock=lambda: 12.5,
    )
    request = _request(
        ExternalActionKind.SEND_REPLY,
        {"text": "hello", "quote_message_id": "platform-user-1"},
    )

    result = await dispatcher.dispatch(request, _claim(request))

    assert adapter.sent[0][0] == _TARGET
    assert [element.type for element in adapter.sent[0][1]] == ["quote", "text"]
    assert result.assistant_message is not None
    assert result.assistant_message.session_id == _TARGET
    assert result.assistant_message.platform_msg_id == "platform-reply-1"
    assert result.assistant_message.created_at == 12_500.0


@pytest.mark.asyncio
async def test_cross_session_quote_is_rejected_before_adapter_send() -> None:
    adapter = _Adapter()
    dispatcher = AdapterExternalActionDispatcher(
        adapters=_Adapters(adapter),
        database=_Database(
            _MessageLogs(
                {
                    7: {
                        "platform_msg_id": "platform-user-1",
                        "session_id": "instance-a:group:another-room",
                    }
                }
            )
        ),
    )
    request = _request(
        ExternalActionKind.SEND_REPLY,
        {"text": "hello", "quote_message_log_id": 7},
    )

    with pytest.raises(ExternalActionPreDispatchRejected, match="session_mismatch"):
        await dispatcher.dispatch(request, _claim(request))

    assert adapter.sent == []
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_poke_and_reaction_use_guarded_adapter_api_shapes() -> None:
    adapter = _Adapter()
    dispatcher = AdapterExternalActionDispatcher(
        adapters=_Adapters(adapter),
        database=_Database(
            _MessageLogs(
                {
                    9: {
                        "platform_msg_id": "platform-user-9",
                        "session_id": _TARGET,
                    }
                }
            )
        ),
    )
    poke = _request(ExternalActionKind.SEND_POKE, {"user_id": "user-a"})
    reaction = _request(
        ExternalActionKind.SEND_REACTION,
        {"action": "remove", "emoji_id": "42", "message_log_id": 9},
    )

    await dispatcher.dispatch(poke, _claim(poke))
    await dispatcher.dispatch(reaction, _claim(reaction))

    assert adapter.calls == [
        ("internal.test.poke", {"group_id": "room-a", "user_id": "user-a"}),
        (
            "reaction.delete",
            {
                "emoji_id": "42",
                "message_id": "platform-user-9",
                "session_id": _TARGET,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_unavailable_adapter_is_a_safe_pre_dispatch_rejection() -> None:
    request = _request(ExternalActionKind.SEND_POKE, {"user_id": "user-a"})
    dispatcher = AdapterExternalActionDispatcher(
        adapters=_Adapters(None),
        database=_Database(_MessageLogs()),
    )

    with pytest.raises(ExternalActionPreDispatchRejected, match="adapter_not_found"):
        await dispatcher.dispatch(request, _claim(request))
