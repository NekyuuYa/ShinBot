"""Tests for fenced Actor v3 Active Chat context projection."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from shinbot.agent.runtime.session_actor.active_chat_workflow_context import (
    ActorActiveChatBootstrapWorkflowContextProjector,
    ActorActiveChatRoundWorkflowContextProjector,
    ActorActiveChatWorkflowContextError,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageConsumptionProvenance,
    MessageLedgerEntry,
    MessagePriorityFlags,
)
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveChatBootstrapWorkflowRequest,
    ActiveChatRoundWorkflowRequest,
    ActorWorkflowEffectInput,
    WorkflowEffectAdapterError,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapDisposition,
)

_KEY = SessionKey("profile-a", "bot-a:instance-a:group:room-a")
_INSTANCE_ID = "instance-a"
_BASE_SESSION_ID = "instance-a:group:room-a"


@dataclass(slots=True)
class _BootstrapLedger:
    """Full-ledger fake used to prove bootstrap never reads unread input."""

    entries: tuple[MessageLedgerEntry, ...]
    calls: list[SessionKey] = field(default_factory=list)

    async def list_message_ledger(
        self,
        key: SessionKey,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Record the full-ledger request and return durable sequence order."""

        self.calls.append(key)
        return self.entries


@dataclass(slots=True)
class _RoundLedger:
    """Dual-fenced unread-ledger fake for one Active Chat round."""

    entries: tuple[MessageLedgerEntry, ...]
    calls: list[tuple[SessionKey, int, int, int]] = field(default_factory=list)

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        ownership_generation: int,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Record both actor fences and return the configured unread projection."""

        self.calls.append(
            (
                key,
                ownership_generation,
                input_watermark,
                input_ledger_sequence,
            )
        )
        return self.entries


@dataclass(slots=True)
class _MessageStore:
    """Message-log fake that returns rows in the opposite order."""

    payloads: dict[int, dict[str, object]]
    requested_ids: list[tuple[int, ...]] = field(default_factory=list)

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[dict[str, object]]:
        """Load only authorized records, deliberately out of durable order."""

        ids = tuple(message_log_ids)
        self.requested_ids.append(ids)
        return [
            self.payloads[message_log_id]
            for message_log_id in reversed(ids)
            if message_log_id in self.payloads
        ]


def _review_consumption(
    operation_id: str,
    *,
    message_log_id: int,
    ledger_sequence: int,
) -> MessageConsumptionProvenance:
    """Build one valid review provenance that covers the supplied ledger row."""

    return MessageConsumptionProvenance(
        consumption_id=f"review-consumption:{message_log_id}",
        idempotency_key=f"review-idempotency:{message_log_id}",
        operation_id=operation_id,
        source_event_id="review-due-a",
        input_watermark=max(100, message_log_id),
        input_ledger_sequence=max(100, ledger_sequence),
        ownership_generation=1,
        committed_at=10.0,
    )


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    review_operation_id: str | None = None,
    sender_id: str | None = None,
) -> MessageLedgerEntry:
    """Build one eligible ledger row, optionally consumed by review."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=1,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id=sender_id or f"user:{message_log_id}",
        instance_id=_INSTANCE_ID,
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=_BASE_SESSION_ID,
        platform="test",
        self_id="bot-a",
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
        review_consumption=(
            _review_consumption(
                review_operation_id,
                message_log_id=message_log_id,
                ledger_sequence=ledger_sequence,
            )
            if review_operation_id is not None
            else None
        ),
    )


def _effect(
    entries: tuple[MessageLedgerEntry, ...] = (),
) -> ActorWorkflowEffectInput:
    """Build one actor-owned workflow effect with a wide enough frozen fence."""

    return ActorWorkflowEffectInput(
        key=_KEY,
        operation_id="active-chat-operation-a",
        effect_id="active-chat-effect-a",
        idempotency_key="active-chat-idempotency-a",
        source_event_id="active-chat-due-a",
        ownership_generation=1,
        instance_id=_INSTANCE_ID,
        target_session_id=_BASE_SESSION_ID,
        input_watermark=100,
        input_ledger_sequence=100,
        ledger_entries=entries,
    )


def _bootstrap_request(
    handoff_message_log_ids: tuple[int, ...],
) -> ActiveChatBootstrapWorkflowRequest:
    """Build a bootstrap request whose input exists only in the full ledger."""

    return ActiveChatBootstrapWorkflowRequest(
        effect=_effect(),
        active_epoch=3,
        handoff_operation_id="review-operation-a",
        handoff_message_log_ids=handoff_message_log_ids,
    )


def _round_request(
    entries: tuple[MessageLedgerEntry, ...],
    *,
    message_log_ids: tuple[int, ...],
) -> ActiveChatRoundWorkflowRequest:
    """Build a round request that may intentionally reverse request ID order."""

    return ActiveChatRoundWorkflowRequest(
        effect=_effect(entries),
        active_epoch=3,
        round_schedule_id="round-schedule-a",
        message_log_ids=message_log_ids,
        interest_value=12.0,
        bootstrap_disposition=ActiveChatBootstrapDisposition.ENGAGED.value,
    )


@pytest.mark.asyncio
async def test_bootstrap_reads_exact_review_handoff_from_full_ledger_in_ledger_order() -> None:
    """Bootstrap reaches only review-consumed handoff records in durable order."""

    full_ledger = (
        _entry(21, ledger_sequence=1, review_operation_id="review-other"),
        _entry(11, ledger_sequence=2, review_operation_id="review-operation-a"),
        _entry(7, ledger_sequence=3, review_operation_id="review-operation-a"),
        _entry(3, ledger_sequence=4),
    )
    ledger = _BootstrapLedger(full_ledger)
    message_store = _MessageStore(
        {
            11: {"id": 11, "session_id": _BASE_SESSION_ID, "raw_text": "first"},
            7: {"id": 7, "session_id": _BASE_SESSION_ID, "raw_text": "second"},
        }
    )
    projector = ActorActiveChatBootstrapWorkflowContextProjector(
        ledger=ledger,
        message_store=message_store,
    )

    stage_input = await projector.build_active_chat_bootstrap_stage_input(
        _bootstrap_request((7, 11))
    )

    assert ledger.calls == [_KEY]
    assert message_store.requested_ids == [(11, 7)]
    assert [message["id"] for message in stage_input.source_messages] == [11, 7]
    assert stage_input.purpose == "active_chat_bootstrap"
    assert stage_input.context_messages == []
    assert stage_input.instruction_content == []
    assert stage_input.metadata["handoff_operation_id"] == "review-operation-a"
    assert stage_input.metadata["handoff_message_log_ids"] == [11, 7]
    assert stage_input.metadata["ledger_message_log_ids"] == [11, 7]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("review_operation_id", "match"),
    [
        (None, "was not consumed by review"),
        ("review-operation-other", "another review operation"),
    ],
)
async def test_bootstrap_rejects_missing_or_wrong_review_handoff_provenance(
    review_operation_id: str | None,
    match: str,
) -> None:
    """A bootstrap handoff must be owned by the exact review operation."""

    ledger = _BootstrapLedger(
        (_entry(11, ledger_sequence=1, review_operation_id=review_operation_id),)
    )
    message_store = _MessageStore({})
    projector = ActorActiveChatBootstrapWorkflowContextProjector(
        ledger=ledger,
        message_store=message_store,
    )

    with pytest.raises(ActorActiveChatWorkflowContextError, match=match):
        await projector.build_active_chat_bootstrap_stage_input(_bootstrap_request((11,)))

    assert message_store.requested_ids == []


@pytest.mark.asyncio
async def test_round_uses_dual_fence_and_restores_exact_ledger_order() -> None:
    """Round context reads the frozen unread projection, not request/store order."""

    snapshot = (
        _entry(21, ledger_sequence=1),
        _entry(10, ledger_sequence=2),
    )
    ledger = _RoundLedger((*snapshot, _entry(5, ledger_sequence=3)))
    message_store = _MessageStore(
        {
            21: {"id": 21, "session_id": _BASE_SESSION_ID, "raw_text": "first"},
            10: {"id": 10, "session_id": _BASE_SESSION_ID, "raw_text": "second"},
        }
    )
    projector = ActorActiveChatRoundWorkflowContextProjector(
        ledger=ledger,
        message_store=message_store,
    )
    request = _round_request(snapshot, message_log_ids=(10, 21))

    stage_input = await projector.build_active_chat_round_stage_input(request)

    assert ledger.calls == [(_KEY, 1, 100, 100)]
    assert message_store.requested_ids == [(21, 10)]
    assert [message["id"] for message in stage_input.source_messages] == [21, 10]
    assert stage_input.purpose == "active_chat_round"
    assert stage_input.metadata["ledger_message_log_ids"] == [21, 10]
    assert stage_input.metadata["candidate_message_ids"] == [21, 10]
    assert stage_input.metadata["message_log_ids"] == [21, 10]
    assert stage_input.metadata["active_chat_interest_value"] == 12.0


@pytest.mark.asyncio
async def test_round_rejects_captured_selection_that_escaped_its_effect_snapshot() -> None:
    """A same-ID row with altered durable content cannot replace frozen input."""

    snapshot = (_entry(21, ledger_sequence=1),)
    changed_row = _entry(21, ledger_sequence=1, sender_id="attacker")
    ledger = _RoundLedger((changed_row,))
    message_store = _MessageStore({})
    projector = ActorActiveChatRoundWorkflowContextProjector(
        ledger=ledger,
        message_store=message_store,
    )

    with pytest.raises(
        ActorActiveChatWorkflowContextError,
        match="captured ledger changed selected input",
    ):
        await projector.build_active_chat_round_stage_input(
            _round_request(snapshot, message_log_ids=(21,))
        )

    assert message_store.requested_ids == []


def test_round_request_rejects_ids_outside_its_frozen_effect_snapshot() -> None:
    """A caller cannot construct a request that widens the selected snapshot."""

    with pytest.raises(
        WorkflowEffectAdapterError,
        match="outside its captured ledger",
    ):
        _round_request((_entry(21, ledger_sequence=1),), message_log_ids=(99,))
