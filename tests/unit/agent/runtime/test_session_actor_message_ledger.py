"""Unit tests for actor-owned message ledger facts and projections."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    ConsumeMessageLedgerEntries,
    MessageConsumptionProvenance,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
    MessageLedgerEntry,
    MessageLedgerProjectionKind,
    MessagePriorityFlags,
    MessageWatermarkDisposition,
    classify_message_watermark,
    count_projected_messages,
    project_message_ranges,
    validate_message_ledger_mutations,
)


def _append(
    message_log_id: int = 10,
    *,
    key: SessionKey | None = None,
    priority: MessagePriorityFlags | None = None,
    **changes: Any,
) -> AppendMessageLedgerEntry:
    values: dict[str, Any] = {
        "key": key or SessionKey("profile-a", "session-a"),
        "message_log_id": message_log_id,
        "ownership_generation": 3,
        "source_event_id": f"message-received:{message_log_id}",
        "actor_event_id": f"message-received:{message_log_id}",
        "delivery_version": 1,
        "event_source": "message_ingress",
        "sender_id": "user-a",
        "instance_id": "instance-a",
        "event_type": "message-created",
        "base_session_id": "base-session",
        "bot_session_id": "session-a",
        "platform": "test",
        "self_id": "bot-a",
        "response_profile": "balanced",
        "priority": priority or MessagePriorityFlags(),
        "causation_id": "route-decision-a",
        "correlation_id": "correlation-a",
        "trace_id": "trace-a",
        "observed_at": float(message_log_id),
        "occurred_at": float(message_log_id),
        "event_created_at": float(message_log_id + 1),
        "metadata": {"nested": {"items": [1, {"ok": True}]}},
    }
    values.update(changes)
    return AppendMessageLedgerEntry(**values)


def _provenance(
    *,
    operation_id: str,
    input_watermark: int = 20,
    input_ledger_sequence: int = 20,
) -> MessageConsumptionProvenance:
    return MessageConsumptionProvenance(
        consumption_id=f"consume:{operation_id}",
        idempotency_key=f"idempotency:{operation_id}",
        operation_id=operation_id,
        source_event_id=f"completion:{operation_id}",
        input_watermark=input_watermark,
        input_ledger_sequence=input_ledger_sequence,
        ownership_generation=3,
        committed_at=50.0,
    )


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int | None = None,
    occurred_at: float | None = None,
    key: SessionKey | None = None,
    review: MessageConsumptionProvenance | None = None,
    chat: MessageConsumptionProvenance | None = None,
    high_priority: MessageConsumptionProvenance | None = None,
    priority: MessagePriorityFlags | None = None,
) -> MessageLedgerEntry:
    append_changes: dict[str, Any] = {}
    if occurred_at is not None:
        append_changes["occurred_at"] = occurred_at
    return MessageLedgerEntry(
        message=_append(
            message_log_id,
            key=key,
            priority=priority,
            **append_changes,
        ),
        ledger_sequence=ledger_sequence or message_log_id,
        recorded_at=40.0,
        updated_at=50.0 if any((review, chat, high_priority)) else 40.0,
        review_consumption=review,
        chat_consumption=chat,
        high_priority_consumption=high_priority,
    )


def test_append_fact_is_canonical_and_deeply_immutable() -> None:
    entry = _append(
        priority=MessagePriorityFlags(
            mention=True,
            should_wake_active_reply=True,
            reasons={"mention": {"count": 2}},
        )
    )

    assert entry.priority.is_high_priority is True
    assert entry.to_record()["profile_id"] == "profile-a"
    assert '"message_log_id":10' in entry.canonical_json

    with pytest.raises(TypeError, match="immutable"):
        entry.metadata["new"] = "value"
    with pytest.raises(TypeError, match="immutable"):
        entry.metadata["nested"]["items"].append(2)
    with pytest.raises(TypeError, match="immutable"):
        entry.priority.reasons["mention"]["count"] = 3


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"message_log_id": True}, "positive integer"),
        ({"ownership_generation": 0}, "positive integer"),
        ({"delivery_version": 1.0}, "positive integer"),
        ({"is_private": 1}, "boolean"),
        ({"occurred_at": float("nan")}, "finite"),
        ({"observed_at": float("inf")}, "finite"),
        ({"actor_event_id": "other-event"}, "must match"),
        ({"instance_id": ""}, "must not be empty"),
        ({"event_source": 1}, "must be strings"),
        ({"metadata": {1: "invalid"}}, "keys must be strings"),
        ({"metadata": {"delay": float("inf")}}, "numbers must be finite"),
    ],
)
def test_append_fact_rejects_ambiguous_durable_values(
    changes: dict[str, Any],
    match: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        _append(**changes)


def test_priority_wake_requires_an_explanatory_priority_flag() -> None:
    with pytest.raises(ValueError, match="requires at least one"):
        MessagePriorityFlags(should_wake_active_reply=True)

    with pytest.raises(TypeError, match="boolean"):
        MessagePriorityFlags(mention=1)  # type: ignore[arg-type]


def test_consumption_is_canonical_watermark_fenced_and_immutable() -> None:
    consumption = ConsumeMessageLedgerEntries(
        key=SessionKey("profile-a", "session-a"),
        kind=MessageLedgerConsumptionKind.REVIEW,
        consumption_id="consume-review-a",
        idempotency_key="review-operation-a:messages",
        operation_id="review-operation-a",
        source_event_id="review-completed-a",
        ownership_generation=3,
        input_watermark=30,
        input_ledger_sequence=30,
        selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
        explicit_message_log_ids=(30, 10, 20),
        occurred_at=90.0,
        metadata={"stage": {"name": "scan"}},
    )

    assert consumption.explicit_message_log_ids == (10, 20, 30)
    assert consumption.covers(20, ledger_sequence=30) is True
    assert consumption.covers(20, ledger_sequence=31) is False
    assert consumption.covers(25, ledger_sequence=25) is False
    assert consumption.covers(31, ledger_sequence=1) is False
    assert '"explicit_message_log_ids":[10,20,30]' in consumption.canonical_json
    with pytest.raises(TypeError, match="immutable"):
        consumption.metadata["stage"]["name"] = "reply"


def test_watermark_selector_is_coarse_and_does_not_prove_persisted_consumption() -> None:
    consumption = ConsumeMessageLedgerEntries(
        key=SessionKey("profile-a", "session-a"),
        kind=MessageLedgerConsumptionKind.CHAT,
        consumption_id="consume-chat-a",
        idempotency_key="chat-operation-a:messages",
        operation_id="chat-operation-a",
        source_event_id="chat-completed-a",
        ownership_generation=3,
        input_watermark=40,
        input_ledger_sequence=4,
        selection=MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK,
    )

    assert consumption.covers(39, ledger_sequence=3) is True
    assert consumption.covers(40, ledger_sequence=4) is True
    assert consumption.covers(39, ledger_sequence=5) is False
    assert consumption.covers(41, ledger_sequence=1) is False
    assert (
        classify_message_watermark(39, input_watermark=40)
        is MessageWatermarkDisposition.CAPTURED_OR_LATE
    )
    assert (
        classify_message_watermark(41, input_watermark=40)
        is MessageWatermarkDisposition.NEW_ACTIVITY
    )


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"ownership_generation": True}, "positive integer"),
        ({"input_watermark": -1}, "non-negative integer"),
        ({"input_ledger_sequence": -1}, "non-negative integer"),
        (
            {
                "selection": MessageLedgerConsumptionSelection.EXPLICIT_IDS,
                "explicit_message_log_ids": (),
            },
            "requires at least one",
        ),
        (
            {
                "selection": (
                    MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK
                ),
                "explicit_message_log_ids": (10,),
            },
            "cannot carry explicit",
        ),
        ({"input_watermark": 10, "explicit_message_log_ids": (11,)}, "exceed"),
        (
            {"input_watermark": 10, "explicit_message_log_ids": (10, 10)},
            "duplicates",
        ),
        ({"occurred_at": float("nan")}, "finite"),
        ({"metadata": {"value": float("inf")}}, "numbers must be finite"),
    ],
)
def test_consumption_rejects_invalid_fences(
    changes: dict[str, Any],
    match: str,
) -> None:
    values: dict[str, Any] = {
        "key": SessionKey("profile-a", "session-a"),
        "kind": MessageLedgerConsumptionKind.REVIEW,
        "consumption_id": "consume-review-a",
        "idempotency_key": "review-operation-a:messages",
        "operation_id": "review-operation-a",
        "source_event_id": "review-completed-a",
        "ownership_generation": 3,
        "input_watermark": 10,
        "input_ledger_sequence": 10,
        "selection": MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK,
    }
    values.update(changes)
    with pytest.raises((TypeError, ValueError), match=match):
        ConsumeMessageLedgerEntries(**values)


def test_ranges_and_counts_are_derived_from_individual_message_rows() -> None:
    chat_consumption = _provenance(operation_id="chat-round-a")
    priority = MessagePriorityFlags(reply_to_bot=True, should_wake_active_reply=True)
    entries = [
        _entry(10, priority=priority),
        _entry(20, chat=chat_consumption),
        _entry(30),
        _entry(40, priority=priority, high_priority=chat_consumption),
    ]

    unread = project_message_ranges(entries)
    assert [
        (item.start_message_log_id, item.end_message_log_id, item.message_count)
        for item in unread
    ] == [(10, 10, 1), (30, 40, 2)]
    assert count_projected_messages(entries) == 3
    assert (
        count_projected_messages(
            entries,
            kind=MessageLedgerProjectionKind.CHAT_PENDING,
        )
        == 3
    )
    assert (
        count_projected_messages(
            entries,
            kind=MessageLedgerProjectionKind.HIGH_PRIORITY_PENDING,
        )
        == 1
    )


def test_review_and_chat_pending_never_reexpose_cross_consumed_messages() -> None:
    entries = [
        _entry(10, review=_provenance(operation_id="review-a")),
        _entry(20, chat=_provenance(operation_id="chat-a")),
        _entry(30),
    ]

    assert (
        count_projected_messages(
            entries,
            kind=MessageLedgerProjectionKind.REVIEW_PENDING,
        )
        == 1
    )
    assert (
        count_projected_messages(
            entries,
            kind=MessageLedgerProjectionKind.CHAT_PENDING,
        )
        == 1
    )


def test_range_projection_uses_ledger_sequence_and_time_extrema() -> None:
    entries = [
        _entry(10, ledger_sequence=2, occurred_at=5.0),
        _entry(30, ledger_sequence=1, occurred_at=30.0),
    ]

    projected = project_message_ranges(entries)

    assert len(projected) == 1
    assert projected[0].start_ledger_sequence == 1
    assert projected[0].end_ledger_sequence == 2
    assert projected[0].start_message_log_id == 30
    assert projected[0].end_message_log_id == 10
    assert projected[0].start_at == 5.0
    assert projected[0].end_at == 30.0


def test_range_projection_fails_closed_when_profiles_are_mixed() -> None:
    entries = [
        _entry(10, key=SessionKey("profile-a", "session-shared")),
        _entry(20, key=SessionKey("profile-b", "session-shared")),
    ]

    with pytest.raises(ValueError, match="cannot mix session keys"):
        project_message_ranges(entries)


def test_entry_rejects_commit_clock_regression() -> None:
    with pytest.raises(ValueError, match="cannot precede"):
        replace(_entry(10), recorded_at=50.0, updated_at=49.0)


def test_transition_batch_fences_key_generation_and_source_event() -> None:
    append = _append()

    assert validate_message_ledger_mutations(
        (append,),
        key=append.key,
        ownership_generation=3,
        source_event_id=append.source_event_id,
    ) == (append,)
    with pytest.raises(ValueError, match="ownership generation"):
        validate_message_ledger_mutations(
            (append,),
            key=append.key,
            ownership_generation=4,
            source_event_id=append.source_event_id,
        )
    with pytest.raises(ValueError, match="source event"):
        validate_message_ledger_mutations(
            (append,),
            key=append.key,
            ownership_generation=3,
            source_event_id="different-event",
        )
    with pytest.raises(ValueError, match="actor key"):
        validate_message_ledger_mutations(
            (append,),
            key=SessionKey("profile-b", append.key.session_id),
            ownership_generation=3,
            source_event_id=append.source_event_id,
        )


def test_ownership_generation_is_a_fence_not_canonical_business_identity() -> None:
    append = _append()
    consumption = ConsumeMessageLedgerEntries(
        key=append.key,
        kind=MessageLedgerConsumptionKind.REVIEW,
        selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
        consumption_id="consume-review-a",
        idempotency_key="review-operation-a:messages",
        operation_id="review-operation-a",
        source_event_id="review-completed-a",
        ownership_generation=3,
        input_watermark=append.message_log_id,
        input_ledger_sequence=1,
        explicit_message_log_ids=(append.message_log_id,),
    )

    assert replace(append, ownership_generation=4).canonical_json == append.canonical_json
    assert (
        replace(consumption, ownership_generation=4).canonical_json
        == consumption.canonical_json
    )
