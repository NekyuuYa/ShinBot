"""Tests for strict Actor v2 active-reply context projection."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import pytest

from shinbot.agent.runtime.session_actor.active_reply_workflow_context import (
    ActorActiveReplyWorkflowContextError,
    ActorActiveReplyWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageLedgerEntry,
    MessagePriorityFlags,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowRequest,
    ActorWorkflowEffectInput,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")
_INSTANCE_ID = "instance-a"
_BASE_SESSION_ID = "instance-a:group:room-a"


@dataclass(slots=True)
class _MessageStore:
    """Message-log fake that records authorized IDs and can return corruption."""

    payloads: dict[int, dict[str, object]]
    requested_ids: list[tuple[int, ...]]
    duplicate_id: int | None = None
    unexpected_payload: dict[str, object] | None = None

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[dict[str, object]]:
        """Return configured logs in reverse order to test ledger restoration."""

        self.requested_ids.append(tuple(message_log_ids))
        result = [
            self.payloads[message_log_id]
            for message_log_id in reversed(message_log_ids)
            if message_log_id in self.payloads
        ]
        if self.duplicate_id is not None:
            result.append(self.payloads[self.duplicate_id])
        if self.unexpected_payload is not None:
            result.append(self.unexpected_payload)
        return result


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    ownership_generation: int = 1,
    eligible_for_work: bool = True,
    base_session_id: str = _BASE_SESSION_ID,
) -> MessageLedgerEntry:
    """Create one durable actor ledger row for a selected active reply."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=ownership_generation,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id="user-a",
        instance_id=_INSTANCE_ID,
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=base_session_id,
        platform="test",
        self_id="bot-a",
        eligible_for_work=eligible_for_work,
        suppression_reason="test_suppressed" if not eligible_for_work else "",
        priority=MessagePriorityFlags(),
        observed_at=float(ledger_sequence),
        occurred_at=float(ledger_sequence),
        event_created_at=float(ledger_sequence),
    )
    return MessageLedgerEntry(
        message=message,
        ledger_sequence=ledger_sequence,
        recorded_at=float(ledger_sequence),
        updated_at=float(ledger_sequence),
    )


def _request(
    entries: tuple[MessageLedgerEntry, ...],
    *,
    message_log_ids: tuple[int, ...] | None = None,
) -> ActiveReplyWorkflowRequest:
    """Create one valid request that can be intentionally corrupted in a test."""

    selected_ids = message_log_ids or tuple(entry.message_log_id for entry in entries)
    effect = ActorWorkflowEffectInput(
        key=_KEY,
        operation_id="operation-a",
        effect_id="effect-a",
        idempotency_key="idempotency-a",
        source_event_id="source-a",
        ownership_generation=1,
        instance_id=_INSTANCE_ID,
        target_session_id=_BASE_SESSION_ID,
        input_watermark=20,
        input_ledger_sequence=5,
        ledger_entries=entries,
    )
    return ActiveReplyWorkflowRequest(
        effect=effect,
        message_log_ids=selected_ids,
        response_profile="fast",
        sender_id="user-a",
    )


@pytest.mark.asyncio
async def test_projector_loads_exact_selected_logs_in_durable_ledger_order() -> None:
    """A reversed request/store order cannot change model-visible ledger order."""

    entries = (_entry(11, ledger_sequence=1), _entry(10, ledger_sequence=2))
    request = _request(entries, message_log_ids=(10, 11))
    message_store = _MessageStore(
        payloads={
            10: {"id": 10, "session_id": _BASE_SESSION_ID, "raw_text": "second"},
            11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "first"},
        },
        requested_ids=[],
    )
    projector = ActorActiveReplyWorkflowContextProjector(message_store=message_store)

    stage_input = await projector.build_active_reply_stage_input(request)

    assert message_store.requested_ids == [(11, 10)]
    assert [message["id"] for message in stage_input.source_messages] == [11, 10]
    assert stage_input.session_id == _KEY.session_id
    assert stage_input.instance_id == _INSTANCE_ID
    assert stage_input.purpose == "reply_decision"
    assert stage_input.metadata == {
        "purpose": "reply_decision",
        "actor_v2": True,
        "operation_id": "operation-a",
        "effect_id": "effect-a",
        "ownership_generation": 1,
        "input_watermark": 20,
        "input_ledger_sequence": 5,
        "target_session_id": _BASE_SESSION_ID,
        "ledger_message_log_ids": [11, 10],
        "candidate_message_ids": [11, 10],
        "response_profile": "fast",
        "sender_id": "user-a",
    }


@pytest.mark.asyncio
async def test_projector_rejects_ledger_entry_with_another_base_session() -> None:
    """A selected ledger row cannot retarget the active-reply transport session."""

    request = _request(
        (_entry(10, ledger_sequence=1, base_session_id="instance-a:other"),)
    )
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="different transport session",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_message_log_with_a_different_base_session() -> None:
    """A message-store row must join to the selected ledger base session exactly."""

    request = _request((_entry(10, ledger_sequence=1),))
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(
            payloads={
                10: {"id": 10, "session_id": "instance-a:other", "raw_text": "bad"}
            },
            requested_ids=[],
        )
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="message log session mismatch: 10",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_fails_when_a_selected_log_disappears() -> None:
    """Missing logs fail the effect instead of silently shrinking model input."""

    request = _request((_entry(10, ledger_sequence=1),))
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="message logs disappeared: 10",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_request_ids_that_do_not_match_selected_entries() -> None:
    """A subset request cannot silently exclude rows from its captured selection."""

    request = _request(
        (_entry(10, ledger_sequence=1), _entry(11, ledger_sequence=2)),
        message_log_ids=(10,),
    )
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="do not match the selected ledger entries",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_selected_entry_from_another_ownership_generation() -> None:
    """A cross-generation ledger row cannot enter a current owner operation."""

    request = _request((_entry(10, ledger_sequence=1),))
    object.__setattr__(
        request.effect,
        "ledger_entries",
        (_entry(10, ledger_sequence=1, ownership_generation=2),),
    )
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="another ownership generation",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_suppressed_selected_entry() -> None:
    """Suppressed input cannot be restored by a corrupted effect snapshot."""

    request = _request((_entry(10, ledger_sequence=1),))
    suppressed_entry = _entry(
        10,
        ledger_sequence=1,
        eligible_for_work=False,
    )
    object.__setattr__(request.effect, "ledger_entries", (suppressed_entry,))
    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="ledger entry is suppressed",
    ):
        await projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_duplicate_or_unexpected_message_store_rows() -> None:
    """The selected store result must be an exact one-to-one ID correspondence."""

    request = _request((_entry(10, ledger_sequence=1),))
    duplicate_projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(
            payloads={
                10: {"id": 10, "session_id": _BASE_SESSION_ID, "raw_text": "one"}
            },
            requested_ids=[],
            duplicate_id=10,
        )
    )
    unexpected_projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(
            payloads={
                10: {"id": 10, "session_id": _BASE_SESSION_ID, "raw_text": "one"}
            },
            requested_ids=[],
            unexpected_payload={
                "id": 99,
                "session_id": _BASE_SESSION_ID,
                "raw_text": "unexpected",
            },
        )
    )

    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="duplicate message",
    ):
        await duplicate_projector.build_active_reply_stage_input(request)
    with pytest.raises(
        ActorActiveReplyWorkflowContextError,
        match="unexpected message",
    ):
        await unexpected_projector.build_active_reply_stage_input(request)


@pytest.mark.asyncio
async def test_projector_rejects_invalid_request_type() -> None:
    """Only the actor-owned active-reply request type may authorize context."""

    projector = ActorActiveReplyWorkflowContextProjector(
        message_store=_MessageStore(payloads={}, requested_ids=[])
    )

    with pytest.raises(TypeError, match="ActiveReplyWorkflowRequest"):
        await projector.build_active_reply_stage_input(cast(ActiveReplyWorkflowRequest, object()))
