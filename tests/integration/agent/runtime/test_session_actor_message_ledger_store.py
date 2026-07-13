"""Store integration coverage for the actor-owned durable message ledger."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    ConsumeMessageLedgerEntries,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
)
from shinbot.agent.runtime.session_actor.message_ledger_persistence import (
    MessageLedgerConflict,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def _make_store(tmp_path: Path) -> tuple[DatabaseManager, SQLiteSessionActorStore]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database, SQLiteSessionActorStore(database, clock=lambda: 100.0)


async def _activate(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    key: SessionKey,
) -> int:
    claim = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="message ledger integration test",
        legacy_session_id=f"legacy:{key.profile_id}:{key.session_id}",
    )
    generation = claim.ownership.generation
    await store.ensure(key, ownership_generation=generation)
    return generation


def _insert_message(
    database: DatabaseManager,
    *,
    token: str,
    created_at: float,
) -> int:
    return database.message_logs.insert(
        MessageLogRecord(
            session_id="base-session",
            platform_msg_id=f"platform:{token}",
            sender_id="user-a",
            sender_name="User A",
            raw_text=token,
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


def _message_event(
    *,
    event_id: str,
    key: SessionKey,
    generation: int,
    message_log_id: int,
    observed_at: float,
    sender_id: str = "user-a",
    is_stopped: bool = False,
    already_handled: bool = False,
    is_mentioned: bool = False,
) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:base-session",
            "bot_session_id": key.session_id,
            "message_log_id": message_log_id,
            "sender_id": sender_id,
            "instance_id": "instance-a",
            "platform": "test",
            "self_id": "bot-a",
            "is_private": False,
            "is_mentioned": is_mentioned,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": already_handled,
            "is_stopped": is_stopped,
            "trace_id": f"trace:{event_id}",
            "observed_at": observed_at,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        source="agent_route_outbox",
        occurred_at=observed_at,
        causation_id=f"route:{event_id}",
        correlation_id=f"correlation:{event_id}",
        trace_id=f"trace:{event_id}",
        available_at=observed_at,
        created_at=observed_at,
    )


def _manual_event(
    *,
    event_id: str,
    key: SessionKey,
    generation: int,
) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="LedgerTestCommand",
        ownership_generation=generation,
        source="integration-test",
        occurred_at=90.0,
        trace_id=f"trace:{event_id}",
        available_at=90.0,
        created_at=90.0,
    )


async def _commit_message(
    store: SQLiteSessionActorStore,
    reducer: AgentSessionReducer,
    event: SessionEventEnvelope,
) -> None:
    await store.enqueue(event)
    claim = await store.claim_next(event.key, worker_id="message-worker")
    assert claim is not None
    aggregate = await store.load(event.key)
    transition = reducer.reduce(aggregate, event)
    await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )


async def _commit_manual(
    store: SQLiteSessionActorStore,
    event: SessionEventEnvelope,
    *,
    operations: tuple[SessionOperation, ...] = (),
    consumptions: tuple[ConsumeMessageLedgerEntries, ...] = (),
) -> None:
    await store.enqueue(event)
    claim = await store.claim_next(event.key, worker_id="manual-worker")
    assert claim is not None
    aggregate = await store.load(event.key)
    data = dict(aggregate.data)
    operation_fences = dict(data.get("operation_fences") or {})
    for operation in operations:
        if (
            operation.input_watermark is not None
            and operation.status
            in {SessionOperationStatus.PENDING, SessionOperationStatus.RUNNING}
        ):
            operation_fences[operation.operation_id] = {
                "input_watermark": operation.input_watermark,
                "input_ledger_sequence": operation.input_ledger_sequence,
            }
    if operation_fences:
        data["operation_fences"] = operation_fences
    target = (
        aggregate.advance(data=data)
        if data != aggregate.data
        else aggregate.advance(state_changed=False)
    )
    await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="ledger_test_command_applied",
            operations=operations,
            message_ledger_mutations=consumptions,
        ),
        expected_revision=aggregate.state_revision,
    )


@pytest.mark.asyncio
async def test_ledger_sequence_restart_and_profile_isolation(tmp_path: Path) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key_a = SessionKey("profile-a", "session-shared")
    key_b = SessionKey("profile-b", "session-shared")
    generation_a = await _activate(database, store, key_a)
    generation_b = await _activate(database, store, key_b)
    earlier_id = _insert_message(database, token="earlier", created_at=5.0)
    later_id = _insert_message(database, token="later", created_at=30.0)

    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-a-later-first",
            key=key_a,
            generation=generation_a,
            message_log_id=later_id,
            observed_at=30.0,
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-a-earlier-second",
            key=key_a,
            generation=generation_a,
            message_log_id=earlier_id,
            observed_at=5.0,
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-b-same-log",
            key=key_b,
            generation=generation_b,
            message_log_id=earlier_id,
            observed_at=5.0,
        ),
    )

    restarted = SQLiteSessionActorStore(database, clock=lambda: 200.0)
    entries_a = await restarted.list_unread_messages(key_a)
    entries_b = await restarted.list_unread_messages(key_b)
    ranges_a = await restarted.list_unread_ranges(key_a)

    assert [(item.ledger_sequence, item.message_log_id) for item in entries_a] == [
        (1, later_id),
        (2, earlier_id),
    ]
    assert [(item.ledger_sequence, item.message_log_id) for item in entries_b] == [
        (1, earlier_id)
    ]
    assert await restarted.count_unread_messages(key_a) == 2
    assert len(ranges_a) == 1
    assert ranges_a[0].start_message_log_id == later_id
    assert ranges_a[0].end_message_log_id == earlier_id
    assert ranges_a[0].start_at == 5.0
    assert ranges_a[0].end_at == 30.0


@pytest.mark.asyncio
async def test_first_actionable_message_atomically_commits_initial_review_schedule(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(database, token="initial", created_at=10.0)
    event = _message_event(
        event_id="message:initial",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=10.0,
    )
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(
            default_review_delay_seconds=30.0,
            default_review_reason="initial integration review",
        )
    )

    await _commit_message(store, reducer, event)

    aggregate = await store.load(key)
    assert aggregate.review_plan_revision == 1
    assert aggregate.review_plan["scheduled_from"] == 100.0
    assert aggregate.review_plan["next_review_at"] == 130.0
    assert aggregate.review_plan["applied_delay_seconds"] == 30.0
    with database.connect() as conn:
        ledger = conn.execute(
            """
            SELECT source_event_id, eligible_for_work
            FROM agent_message_ledger
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        schedule = conn.execute(
            """
            SELECT status, plan_revision, outcome, reason, scheduled_from,
                   next_review_at, available_at, delivery_cycle
            FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            (aggregate.current_plan_id,),
        ).fetchone()
        journal = conn.execute(
            """
            SELECT event_id, event_type, plan_id, outcome,
                   scheduled_from, next_review_at
            FROM agent_review_schedule_events
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT status FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, event.event_id),
        ).fetchone()
        state_transition = conn.execute(
            """
            SELECT event_id, disposition, plan_id
            FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert ledger is not None
    assert tuple(ledger) == (event.event_id, 1)
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        1,
        "defaulted",
        "initial integration review",
        100.0,
        130.0,
        130.0,
        0,
    )
    assert journal is not None
    assert tuple(journal) == (
        event.event_id,
        "scheduled",
        aggregate.current_plan_id,
        "defaulted",
        100.0,
        130.0,
    )
    assert mailbox is not None
    assert str(mailbox["status"]) == "completed"
    assert state_transition is not None
    assert tuple(state_transition) == (
        event.event_id,
        "message_recorded",
        aggregate.current_plan_id,
    )


@pytest.mark.asyncio
async def test_captured_unread_projection_respects_both_operation_boundaries(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    first_id = _insert_message(database, token="first", created_at=10.0)
    second_id = _insert_message(database, token="second", created_at=20.0)
    third_id = _insert_message(database, token="third", created_at=30.0)

    # Delivery order is intentionally different from message-log order. The
    # workflow fence must constrain both durable coordinates.
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-second-first",
            key=key,
            generation=generation,
            message_log_id=second_id,
            observed_at=20.0,
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-first-second",
            key=key,
            generation=generation,
            message_log_id=first_id,
            observed_at=10.0,
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message-third-third",
            key=key,
            generation=generation,
            message_log_id=third_id,
            observed_at=30.0,
        ),
    )

    captured = await store.list_captured_unread(
        key=key,
        input_watermark=second_id,
        input_ledger_sequence=2,
    )
    by_sequence = await store.list_captured_unread(
        key=key,
        input_watermark=third_id,
        input_ledger_sequence=1,
    )
    by_watermark = await store.list_captured_unread(
        key=key,
        input_watermark=first_id,
        input_ledger_sequence=2,
    )

    assert [item.message_log_id for item in captured] == [second_id, first_id]
    assert [item.message_log_id for item in by_sequence] == [second_id]
    assert [item.message_log_id for item in by_watermark] == [first_id]


@pytest.mark.asyncio
async def test_operation_snapshot_includes_same_transition_append_and_stamps_all_fences(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(database, token="wake", created_at=10.0)
    event = _message_event(
        event_id="message-starts-workflow",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=10.0,
    )
    await store.enqueue(event)
    claim = await store.claim_next(key, worker_id="workflow-worker")
    assert claim is not None
    aggregate = await store.load(key)
    base = reducer.reduce(aggregate, event)
    operation_id = "active-reply-operation-a"
    data = dict(base.aggregate.data)
    data["operation_fences"] = {
        operation_id: {
            "input_watermark": message_log_id,
            "input_ledger_sequence": None,
        }
    }
    target = replace(base.aggregate, data=data)
    operation = SessionOperation(
        operation_id=operation_id,
        kind="active_reply",
        status=SessionOperationStatus.PENDING,
        launched_by_event_id=event.event_id,
        state_revision=target.state_revision,
        active_epoch=target.active_epoch,
        activity_generation=target.activity_generation,
        input_watermark=message_log_id,
        started_at=event.occurred_at,
    )
    contract = builtin_effect_contract("run_idle_review_planning")
    effect = SessionEffect(
        effect_id="workflow-effect-a",
        kind=contract.effect_kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        operation_id=operation_id,
        payload={
            "plan_id": target.current_plan_id,
            "active_epoch": target.active_epoch,
            "activity_generation": target.activity_generation,
            "input_watermark": message_log_id,
            "input_ledger_sequence": None,
            "completion_event_id": "workflow-completed-a",
            "failure_event_id": "workflow-failed-a",
            "source": "operation-snapshot-test",
            "trigger": "same-transition-append",
        },
    )

    committed = await store.commit(
        claim,
        replace(
            base,
            aggregate=target,
            operations=(operation,),
            effects=(effect,),
        ),
        expected_revision=aggregate.state_revision,
    )

    committed_fence = committed.data["operation_fences"][operation_id]
    assert committed_fence == {
        "input_watermark": message_log_id,
        "input_ledger_sequence": 1,
    }
    with database.connect() as conn:
        operation_row = conn.execute(
            """
            SELECT input_watermark, input_ledger_sequence
            FROM agent_session_operations WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        effect_row = conn.execute(
            """
            SELECT payload_json FROM agent_effect_outbox WHERE effect_id = ?
            """,
            (effect.effect_id,),
        ).fetchone()
    assert operation_row is not None
    assert tuple(operation_row) == (message_log_id, 1)
    assert effect_row is not None
    effect_payload = json.loads(str(effect_row["payload_json"]))
    assert effect_payload["input_watermark"] == message_log_id
    assert effect_payload["input_ledger_sequence"] == 1


@pytest.mark.asyncio
async def test_conflicting_duplicate_rolls_back_aggregate_and_ledger(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(database, token="duplicate", created_at=10.0)
    first = _message_event(
        event_id="message-original",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=10.0,
    )
    await _commit_message(store, reducer, first)
    before = await store.load(key)
    conflict = _message_event(
        event_id="message-conflicting-route",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=11.0,
    )
    await store.enqueue(conflict)
    claim = await store.claim_next(key, worker_id="conflict-worker")
    assert claim is not None
    transition = reducer.reduce(before, conflict)

    with pytest.raises(MessageLedgerConflict):
        await store.commit(
            claim,
            transition,
            expected_revision=before.state_revision,
        )

    after = await store.load(key)
    assert after.state_revision == before.state_revision
    assert after.event_sequence == before.event_sequence
    assert len(await store.list_message_ledger(key)) == 1
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT status, claim_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, conflict.event_id),
        ).fetchone()
    assert mailbox is not None
    assert str(mailbox["status"]) == "processing"
    assert str(mailbox["claim_id"]) == claim.claim_id


@pytest.mark.asyncio
async def test_explicit_consumption_is_operation_fenced_and_cross_channel_unread(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_ids = [
        _insert_message(database, token=f"message-{index}", created_at=float(index))
        for index in range(1, 4)
    ]
    for message_log_id in message_ids:
        await _commit_message(
            store,
            reducer,
            _message_event(
                event_id=f"message:{message_log_id}",
                key=key,
                generation=generation,
                message_log_id=message_log_id,
                observed_at=float(message_log_id),
            ),
        )

    operation = SessionOperation(
        operation_id="review-operation-a",
        kind="review",
        status=SessionOperationStatus.PENDING,
        launched_by_event_id="review-started",
        state_revision=(await store.load(key)).state_revision,
        active_epoch=0,
        activity_generation=0,
        input_watermark=message_ids[-1],
        started_at=80.0,
    )
    await _commit_manual(
        store,
        _manual_event(
            event_id="review-started",
            key=key,
            generation=generation,
        ),
        operations=(operation,),
    )
    completion = _manual_event(
        event_id="review-completed",
        key=key,
        generation=generation,
    )
    consumption = ConsumeMessageLedgerEntries(
        key=key,
        kind=MessageLedgerConsumptionKind.REVIEW,
        selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
        consumption_id="review-consumption-a",
        idempotency_key="review-operation-a:messages",
        operation_id=operation.operation_id,
        source_event_id=completion.event_id,
        ownership_generation=generation,
        input_watermark=message_ids[-1],
        input_ledger_sequence=3,
        explicit_message_log_ids=tuple(message_ids[:2]),
        reason="review scan consumed explicit candidates",
        trace_id=completion.trace_id,
        occurred_at=completion.occurred_at,
    )
    await _commit_manual(
        store,
        completion,
        operations=(
            replace(
                operation,
                status=SessionOperationStatus.COMPLETED,
                finished_at=completion.occurred_at,
            ),
        ),
        consumptions=(consumption,),
    )

    unread = await store.list_unread_messages(key)
    all_entries = await store.list_message_ledger(key)
    assert [item.message_log_id for item in unread] == [message_ids[-1]]
    assert await store.count_unread_messages(key) == 1
    assert all_entries[0].review_consumption is not None
    assert all_entries[0].review_consumption.operation_id == operation.operation_id
    assert all_entries[0].review_consumption.input_ledger_sequence == 3
    assert all_entries[1].review_consumption is not None
    assert all_entries[2].review_consumption is None
    assert all_entries[2].chat_consumption is None
    duplicate = await store.enqueue(completion)
    assert duplicate.inserted is False

    before_conflict = await store.load(key)
    conflicting_event = _manual_event(
        event_id="review-conflicting-replay",
        key=key,
        generation=generation,
    )
    conflicting_consumption = replace(
        consumption,
        consumption_id="review-consumption-conflict",
        source_event_id=conflicting_event.event_id,
        explicit_message_log_ids=(message_ids[-1],),
    )
    with pytest.raises(MessageLedgerConflict, match="idempotency key"):
        await _commit_manual(
            store,
            conflicting_event,
            consumptions=(conflicting_consumption,),
        )
    after_conflict = await store.load(key)
    assert after_conflict.event_sequence == before_conflict.event_sequence
    assert [
        item.message_log_id for item in await store.list_unread_messages(key)
    ] == [message_ids[-1]]


@pytest.mark.asyncio
async def test_explicit_consumption_rejects_late_append_below_message_watermark(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    first_id = _insert_message(database, token="first", created_at=1.0)
    late_old_id = _insert_message(database, token="late-old", created_at=2.0)
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:first",
            key=key,
            generation=generation,
            message_log_id=first_id,
            observed_at=1.0,
        ),
    )
    operation = SessionOperation(
        operation_id="review-operation-a",
        kind="review",
        status=SessionOperationStatus.PENDING,
        launched_by_event_id="review-started",
        state_revision=(await store.load(key)).state_revision,
        active_epoch=0,
        activity_generation=0,
        input_watermark=late_old_id,
        started_at=80.0,
    )
    await _commit_manual(
        store,
        _manual_event(
            event_id="review-started",
            key=key,
            generation=generation,
        ),
        operations=(operation,),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:late-old",
            key=key,
            generation=generation,
            message_log_id=late_old_id,
            observed_at=2.0,
        ),
    )
    completion = _manual_event(
        event_id="review-completed",
        key=key,
        generation=generation,
    )
    consumption = ConsumeMessageLedgerEntries(
        key=key,
        kind=MessageLedgerConsumptionKind.REVIEW,
        selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
        consumption_id="review-consumption-a",
        idempotency_key="review-operation-a:messages",
        operation_id=operation.operation_id,
        source_event_id=completion.event_id,
        ownership_generation=generation,
        input_watermark=late_old_id,
        input_ledger_sequence=1,
        explicit_message_log_ids=(late_old_id,),
    )

    with pytest.raises(MessageLedgerConflict, match="ledger boundary"):
        await _commit_manual(
            store,
            completion,
            operations=(
                replace(
                    operation,
                    status=SessionOperationStatus.COMPLETED,
                    finished_at=completion.occurred_at,
                ),
            ),
            consumptions=(consumption,),
        )

    assert [
        item.message_log_id for item in await store.list_unread_messages(key)
    ] == [first_id, late_old_id]
    with database.connect() as conn:
        operation_row = conn.execute(
            """
            SELECT status, input_watermark, input_ledger_sequence
            FROM agent_session_operations WHERE operation_id = ?
            """,
            (operation.operation_id,),
        ).fetchone()
    assert operation_row is not None
    assert tuple(operation_row) == ("pending", late_old_id, 1)


@pytest.mark.asyncio
async def test_all_through_snapshot_never_consumes_later_appends(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    first_id = _insert_message(database, token="first", created_at=1.0)
    late_old_id = _insert_message(database, token="late-old", created_at=2.0)
    new_id = _insert_message(database, token="new", created_at=3.0)
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:first",
            key=key,
            generation=generation,
            message_log_id=first_id,
            observed_at=1.0,
        ),
    )
    operation = SessionOperation(
        operation_id="chat-operation-a",
        kind="active_chat_round",
        status=SessionOperationStatus.COMPLETED,
        launched_by_event_id="chat-completed",
        state_revision=(await store.load(key)).state_revision,
        active_epoch=0,
        activity_generation=0,
        input_watermark=late_old_id,
        started_at=70.0,
        finished_at=90.0,
    )
    completion = _manual_event(
        event_id="chat-completed",
        key=key,
        generation=generation,
    )
    consumption = ConsumeMessageLedgerEntries(
        key=key,
        kind=MessageLedgerConsumptionKind.CHAT,
        selection=MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK,
        consumption_id="chat-consumption-a",
        idempotency_key="chat-operation-a:messages",
        operation_id=operation.operation_id,
        source_event_id=completion.event_id,
        ownership_generation=generation,
        input_watermark=late_old_id,
        input_ledger_sequence=1,
        reason="complete snapshot consumed through watermark",
        trace_id=completion.trace_id,
        occurred_at=completion.occurred_at,
    )
    await _commit_manual(
        store,
        completion,
        operations=(operation,),
        consumptions=(consumption,),
    )

    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:late-old",
            key=key,
            generation=generation,
            message_log_id=late_old_id,
            observed_at=2.0,
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:new",
            key=key,
            generation=generation,
            message_log_id=new_id,
            observed_at=3.0,
        ),
    )

    entries = await store.list_message_ledger(key)
    by_id = {item.message_log_id: item for item in entries}
    assert by_id[first_id].chat_consumption is not None
    assert by_id[late_old_id].chat_consumption is None
    assert by_id[new_id].chat_consumption is None
    assert [item.message_log_id for item in await store.list_unread_messages(key)] == [
        late_old_id,
        new_id
    ]


@pytest.mark.asyncio
async def test_suppressed_messages_remain_auditable_but_never_pending(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    self_id = _insert_message(database, token="self", created_at=1.0)
    stopped_id = _insert_message(database, token="stopped", created_at=2.0)

    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:self",
            key=key,
            generation=generation,
            message_log_id=self_id,
            observed_at=1.0,
            sender_id="bot-a",
        ),
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:stopped",
            key=key,
            generation=generation,
            message_log_id=stopped_id,
            observed_at=2.0,
            is_stopped=True,
        ),
    )

    all_entries = await store.list_message_ledger(key)
    assert len(all_entries) == 2
    assert [item.message.eligible_for_work for item in all_entries] == [False, False]
    assert [item.message.suppression_reason for item in all_entries] == [
        "self_message",
        "stopped",
    ]
    assert await store.list_unread_messages(key) == ()
    assert await store.count_unread_messages(key) == 0
    assert await store.list_unread_ranges(key) == ()


@pytest.mark.asyncio
async def test_message_received_cannot_commit_without_exactly_one_append(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(database, token="required", created_at=1.0)
    event = _message_event(
        event_id="message:required",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=1.0,
    )
    await store.enqueue(event)
    claim = await store.claim_next(key, worker_id="missing-append-worker")
    assert claim is not None
    aggregate = await store.load(key)

    with pytest.raises(MessageLedgerConflict, match="exactly one"):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=aggregate.advance(state_changed=False),
                disposition="invalid_message_without_append",
            ),
            expected_revision=aggregate.state_revision,
        )

    assert (await store.load(key)).event_sequence == aggregate.event_sequence
    assert await store.list_message_ledger(key) == ()


@pytest.mark.asyncio
async def test_non_message_event_cannot_smuggle_a_ledger_append(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(database, token="smuggled", created_at=1.0)
    message_event = _message_event(
        event_id="message:source",
        key=key,
        generation=generation,
        message_log_id=message_log_id,
        observed_at=1.0,
    )
    aggregate = await store.load(key)
    append = reducer.reduce(aggregate, message_event).message_ledger_mutations[0]
    command = _manual_event(
        event_id="manual:smuggle",
        key=key,
        generation=generation,
    )
    await store.enqueue(command)
    claim = await store.claim_next(key, worker_id="smuggle-worker")
    assert claim is not None

    with pytest.raises(MessageLedgerConflict, match="only MessageReceived"):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=aggregate.advance(state_changed=False),
                disposition="invalid_non_message_append",
                message_ledger_mutations=(append,),
            ),
            expected_revision=aggregate.state_revision,
        )

    assert await store.list_message_ledger(key) == ()


@pytest.mark.asyncio
async def test_active_reply_completion_atomically_consumes_and_enqueues_action(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_log_id = _insert_message(
        database,
        token="priority",
        created_at=1.0,
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:priority",
            key=key,
            generation=generation,
            message_log_id=message_log_id,
            observed_at=1.0,
            is_mentioned=True,
        ),
    )
    active_reply = await store.load(key)
    operation_id = active_reply.active_reply_operation_id
    fence = active_reply.data["operation_fences"][operation_id]
    contract = builtin_effect_contract("run_active_reply_workflow")
    completion = SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=key,
        kind=AgentSessionEventKind.ACTIVE_REPLY_COMPLETED,
        ownership_generation=generation,
        source=contract.completion_source,
        occurred_at=2.0,
        causation_id=str(fence["source_event_id"]),
        trace_id="trace:active-reply-completed",
        payload={
            "effect_id": fence["effect_id"],
            "effect_kind": contract.effect_kind,
            "idempotency_key": fence["idempotency_key"],
            "operation_id": operation_id,
            "plan_id": fence["plan_id"],
            "active_epoch": fence["active_epoch"],
            "activity_generation": fence["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": fence["input_ledger_sequence"],
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "workflow_result": {
                "schema_version": 1,
                "completion_type": "active_reply",
                "consumed_message_log_ids": [message_log_id],
                "external_actions": {
                    "schema_version": 1,
                    "intents": [
                        {
                            "kind": "send_reply",
                            "proposal_id": "tool-call-a",
                            "action_ordinal": 0,
                            "payload": {"text": "durable reply"},
                        }
                    ],
                },
            },
        },
    )
    await store.enqueue(completion)
    claim = await store.claim_next(key, worker_id="completion-worker")
    assert claim is not None
    committed = await store.commit(
        claim,
        reducer.reduce(active_reply, completion),
        expected_revision=active_reply.state_revision,
    )

    assert committed.state == "idle"
    assert committed.active_reply_operation_id == ""
    entries = await store.list_message_ledger(key)
    assert len(entries) == 1
    assert entries[0].chat_consumption is not None
    assert entries[0].high_priority_consumption is not None
    assert entries[0].chat_consumption.input_ledger_sequence == 1
    assert await store.list_unread_messages(key) == ()
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, input_watermark, input_ledger_sequence
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        action = conn.execute(
            """
            SELECT kind, operation_id, payload_json
            FROM agent_effect_outbox
            WHERE kind = 'send_reply'
            """
        ).fetchone()
    assert operation is not None
    assert tuple(operation) == ("completed", message_log_id, 1)
    assert action is not None
    assert str(action["operation_id"]) == operation_id
    payload = json.loads(str(action["payload_json"]))
    assert payload["instance_id"] == "instance-a"
    assert payload["target_session_id"] == "instance-a:base-session"
    assert payload["payload"] == {"text": "durable reply"}


@pytest.mark.asyncio
@pytest.mark.parametrize("enter_active_chat", (True, False))
async def test_review_completion_atomically_consumes_and_settles_next_state(
    tmp_path: Path,
    enter_active_chat: bool,
) -> None:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    generation = await _activate(database, store, key)
    message_ids = [
        _insert_message(database, token=f"review-{index}", created_at=float(index))
        for index in (1, 2)
    ]
    for index, message_log_id in enumerate(message_ids, start=1):
        await _commit_message(
            store,
            reducer,
            _message_event(
                event_id=f"message:review:{index}",
                key=key,
                generation=generation,
                message_log_id=message_log_id,
                observed_at=float(index),
            ),
        )

    plan_event = _manual_event(
        event_id="plan:review-a",
        key=key,
        generation=generation,
    )
    await store.enqueue(plan_event)
    plan_claim = await store.claim_next(key, worker_id="plan-worker")
    assert plan_claim is not None
    before_plan = await store.load(key)
    plan_id = "review-plan-a"
    plan_revision = before_plan.review_plan_revision + 1
    plan = {
        "plan_id": plan_id,
        "plan_revision": plan_revision,
        "trigger": "integration",
        "kind": "planned",
        "applied_delay_seconds": 60.0,
        "reason": "integration review",
        "source": "integration-test",
    }
    planned = await store.commit(
        plan_claim,
        SessionTransition(
            aggregate=before_plan.advance(
                current_plan_id=plan_id,
                review_plan_revision=plan_revision,
                review_plan=plan,
            ),
            disposition="review_plan_created",
            caused_plan_id=plan_id,
            review_schedules=(
                SessionReviewSchedule(
                    plan_id=plan_id,
                    plan_revision=plan_revision,
                    applied_delay_seconds=60.0,
                    trigger="integration",
                    outcome="planned",
                    source="integration-test",
                    reason="integration review",
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="review-plan-a-scheduled",
                    event_type="scheduled",
                    plan_id=plan_id,
                    previous_plan_id=before_plan.current_plan_id,
                    trigger="integration",
                    outcome="planned",
                    source="integration-test",
                    applied_delay_seconds=60.0,
                    reason="integration review",
                    metadata={
                        "plan_revision": plan_revision,
                        "schedule_outcome": {
                            "active_reply_threshold": {},
                            "applied_delay_seconds": 60.0,
                            "fallback_reason": "",
                            "kind": "planned",
                            "mention_sensitivity": "normal",
                            "model_execution_id": "",
                            "prompt_signature": "",
                            "reason": "integration review",
                            "requested_delay_seconds": None,
                            "source": "integration-test",
                        },
                    },
                ),
            ),
        ),
        expected_revision=before_plan.state_revision,
    )
    due_event_id = review_due_event_id(
        key=key,
        plan_id=plan_id,
        plan_revision=plan_revision,
        ownership_generation=generation,
        delivery_cycle=0,
    )
    due = SessionEventEnvelope(
        event_id=due_event_id,
        key=key,
        kind=AgentSessionEventKind.REVIEW_DUE,
        ownership_generation=generation,
        source=REVIEW_DUE_EVENT_SOURCE,
        occurred_at=100.0,
        payload={
            "version": 1,
            "event_id": due_event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "ownership_generation": generation,
            "attempt_count": 0,
        },
    )
    await store.enqueue(due)
    due_claim = await store.claim_next(key, worker_id="review-worker")
    assert due_claim is not None
    reviewing = await store.commit(
        due_claim,
        reducer.reduce(planned, due),
        expected_revision=planned.state_revision,
    )
    operation_id = reviewing.review_operation_id
    fence = reviewing.data["operation_fences"][operation_id]
    assert fence["input_ledger_sequence"] == 2
    contract = builtin_effect_contract("run_review_workflow")
    completion = SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=key,
        kind=AgentSessionEventKind.REVIEW_COMPLETED,
        ownership_generation=generation,
        source=contract.completion_source,
        occurred_at=110.0,
        causation_id=str(fence["source_event_id"]),
        trace_id="trace:review-completed",
        payload={
            "effect_id": fence["effect_id"],
            "effect_kind": contract.effect_kind,
            "idempotency_key": fence["idempotency_key"],
            "operation_id": operation_id,
            "plan_id": fence["plan_id"],
            "active_epoch": fence["active_epoch"],
            "activity_generation": fence["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": fence["input_ledger_sequence"],
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "workflow_result": {
                "schema_version": 1,
                "completion_type": "review",
                "consumed_message_log_ids": message_ids,
                "external_actions": {
                    "schema_version": 1,
                    "intents": [],
                },
                "enter_active_chat": enter_active_chat,
                "next_review_outcome": (
                    None
                    if enter_active_chat
                    else {
                        "kind": "planned",
                        "applied_delay_seconds": 42.0,
                        "requested_delay_seconds": 42.0,
                        "reason": "review complete",
                        "fallback_reason": "",
                    }
                ),
            },
        },
    )
    await store.enqueue(completion)
    completion_claim = await store.claim_next(key, worker_id="review-worker")
    assert completion_claim is not None
    settled = await store.commit(
        completion_claim,
        reducer.reduce(reviewing, completion),
        expected_revision=reviewing.state_revision,
    )

    if enter_active_chat:
        assert settled.state == "active_chat"
        assert settled.active_epoch == 1
        assert settled.current_plan_id == plan_id
    else:
        assert settled.state == "idle"
        assert settled.active_epoch == 0
        assert settled.current_plan_id != plan_id
        assert settled.review_plan_revision == plan_revision + 1
        assert settled.review_plan["kind"] == "planned"
        assert settled.review_plan["applied_delay_seconds"] == 42.0
        assert settled.review_plan["scheduled_from"] == 100.0
        assert settled.review_plan["next_review_at"] == 142.0
    assert settled.review_operation_id == ""
    entries = await store.list_message_ledger(key)
    assert all(item.review_consumption is not None for item in entries)
    assert all(
        item.review_consumption.input_ledger_sequence == 2
        for item in entries
        if item.review_consumption is not None
    )
    assert await store.list_unread_messages(key) == ()
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, input_watermark, input_ledger_sequence
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        previous_schedule = conn.execute(
            """
            SELECT status FROM agent_review_schedules WHERE plan_id = ?
            """,
            (plan_id,),
        ).fetchone()
        current_schedule = conn.execute(
            """
            SELECT status, applied_delay_seconds
            FROM agent_review_schedules WHERE plan_id = ?
            """,
            (settled.current_plan_id,),
        ).fetchone()
    assert operation is not None
    assert tuple(operation) == ("completed", max(message_ids), 2)
    assert previous_schedule is not None
    assert str(previous_schedule["status"]) == (
        "completed" if enter_active_chat else "superseded"
    )
    assert current_schedule is not None
    assert tuple(current_schedule) == (
        ("completed", 60.0) if enter_active_chat else ("scheduled", 42.0)
    )


@pytest.mark.asyncio
async def test_review_cancellation_completion_releases_stamped_active_reply_effect(
    tmp_path: Path,
) -> None:
    """The delayed reply effect keeps the committed ledger snapshot boundary."""

    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-cancel-review")
    generation = await _activate(database, store, key)
    baseline_message_id = _insert_message(
        database,
        token="baseline",
        created_at=10.0,
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:baseline",
            key=key,
            generation=generation,
            message_log_id=baseline_message_id,
            observed_at=10.0,
        ),
    )

    review_setup = _manual_event(
        event_id="setup:review",
        key=key,
        generation=generation,
    )
    await store.enqueue(review_setup)
    setup_claim = await store.claim_next(key, worker_id="review-setup-worker")
    assert setup_claim is not None
    idle = await store.load(key)
    review_operation_id = "review-operation:cancel-test"
    review_effect_id = "review-effect:cancel-test"
    review_contract = builtin_effect_contract("run_review_workflow")
    plan_id = idle.current_plan_id
    plan_revision = idle.review_plan_revision
    review_fence = {
        "operation_id": review_operation_id,
        "operation_kind": "review",
        "source_event_id": review_setup.event_id,
        "effect_id": review_effect_id,
        "effect_kind": review_contract.effect_kind,
        "idempotency_key": review_effect_id,
        "completion_event_id": "review-completed:cancel-test",
        "failure_event_id": "review-failed:cancel-test",
        "ownership_generation": generation,
        "plan_id": plan_id,
        "plan_revision": plan_revision,
        "active_epoch": 0,
        "activity_generation": 0,
        "input_watermark": baseline_message_id,
        "input_ledger_sequence": None,
        "instance_id": "instance-a",
        "target_session_id": "instance-a:base-session",
    }
    setup_data = dict(idle.data)
    setup_data["operation_fences"] = {review_operation_id: review_fence}
    reviewing_target = idle.advance(
        state=AgentSessionState.REVIEW.value,
        review_operation_id=review_operation_id,
        data=setup_data,
        updated_at=idle.updated_at,
    )
    reviewing = await store.commit(
        setup_claim,
        SessionTransition(
            aggregate=reviewing_target,
            disposition="setup_review_for_cancellation",
            operations=(
                SessionOperation(
                    operation_id=review_operation_id,
                    kind="review",
                    status=SessionOperationStatus.PENDING,
                    launched_by_event_id=review_setup.event_id,
                    state_revision=reviewing_target.state_revision,
                    active_epoch=0,
                    activity_generation=0,
                    input_watermark=baseline_message_id,
                    started_at=review_setup.occurred_at,
                ),
            ),
            effects=(
                SessionEffect(
                    effect_id=review_effect_id,
                    kind=review_contract.effect_kind,
                    contract_version=review_contract.version,
                    contract_signature=review_contract.signature,
                    idempotency_key=review_effect_id,
                    operation_id=review_operation_id,
                    payload={
                        **review_fence,
                        "review_plan": dict(reviewing_target.review_plan),
                    },
                ),
            ),
        ),
        expected_revision=idle.state_revision,
    )
    assert reviewing.data["operation_fences"][review_operation_id][
        "input_ledger_sequence"
    ] == 1

    priority_message_id = _insert_message(
        database,
        token="priority",
        created_at=20.0,
    )
    priority_event = _message_event(
        event_id="message:priority",
        key=key,
        generation=generation,
        message_log_id=priority_message_id,
        observed_at=20.0,
        is_mentioned=True,
    )
    await store.enqueue(priority_event)
    priority_claim = await store.claim_next(key, worker_id="priority-worker")
    assert priority_claim is not None
    interrupted = reducer.reduce(reviewing, priority_event)
    assert [effect.kind for effect in interrupted.effects] == [
        "cancel_review_workflow"
    ]
    waiting = await store.commit(
        priority_claim,
        interrupted,
        expected_revision=reviewing.state_revision,
    )
    active_reply_operation_id = waiting.active_reply_operation_id
    active_reply_fence = waiting.data["operation_fences"][active_reply_operation_id]
    assert active_reply_fence["input_ledger_sequence"] == 2
    cancellation_intent = waiting.data["effect_control_intents"][
        "cancel_review_workflow"
    ]

    cancellation_contract = builtin_effect_contract("cancel_review_workflow")
    cancellation_completion = SessionEventEnvelope(
        event_id=str(cancellation_intent["completion_event_id"]),
        key=key,
        kind=AgentSessionEventKind.REVIEW_CANCELLATION_COMPLETED,
        ownership_generation=generation,
        source=cancellation_contract.completion_source,
        occurred_at=30.0,
        causation_id=str(cancellation_intent["causation_id"]),
        correlation_id=str(cancellation_intent["operation_id"]),
        trace_id="trace:review-cancellation-completed",
        payload={
            "effect_id": cancellation_intent["effect_id"],
            "effect_kind": cancellation_contract.effect_kind,
            "idempotency_key": cancellation_intent["idempotency_key"],
            "operation_id": cancellation_intent["operation_id"],
            "plan_id": cancellation_intent["plan_id"],
            "active_epoch": cancellation_intent["active_epoch"],
            "activity_generation": cancellation_intent["activity_generation"],
            "input_watermark": cancellation_intent["input_watermark"],
            "input_ledger_sequence": cancellation_intent[
                "input_ledger_sequence"
            ],
            "attempt_count": 1,
            "contract_version": cancellation_intent["contract_version"],
            "contract_signature": cancellation_intent["contract_signature"],
        },
    )
    await store.enqueue(cancellation_completion)
    completion_claim = await store.claim_next(key, worker_id="cancellation-worker")
    assert completion_claim is not None
    persisted_waiting = await store.load(key)
    released = await store.commit(
        completion_claim,
        reducer.reduce(persisted_waiting, cancellation_completion),
        expected_revision=persisted_waiting.state_revision,
    )

    assert released.active_reply_operation_id == active_reply_operation_id
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT event_id, operation_id, payload_json
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (active_reply_fence["effect_id"],),
        ).fetchone()
        operation = conn.execute(
            """
            SELECT status, input_ledger_sequence
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (active_reply_operation_id,),
        ).fetchone()
    assert row is not None
    payload = json.loads(str(row["payload_json"]))
    assert str(row["event_id"]) == cancellation_completion.event_id
    assert str(row["operation_id"]) == active_reply_operation_id
    assert payload["input_ledger_sequence"] == 2
    assert payload["message_log_ids"] == [priority_message_id]
    assert operation is not None
    assert tuple(operation) == ("pending", 2)
