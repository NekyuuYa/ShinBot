from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectExecutionContract,
    EffectLane,
    builtin_effect_contract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.events import (
    EventEnqueueResult,
    MailboxEventStatus,
    ReviewScheduleStatus,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.store import (
    AggregateVersionConflict,
    DurableRecordConflict,
    MailboxEventConflict,
    MailboxLeaseConflict,
    SQLiteSessionActorStore,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager

_TEST_EFFECT_CONTRACTS = {
    kind: EffectExecutionContract(
        effect_kind=kind,
        version=1,
        lane=EffectLane.DEFAULT,
        completion_event_kind="TestEffectCompleted",
    )
    for kind in ("schedule_review_timer", "start_workflow")
}
_TEST_EFFECT_AUTHORITY = EffectContractAuthority(
    (*builtin_session_actor_effect_contracts(), *_TEST_EFFECT_CONTRACTS.values())
)


class _OwnershipTestStore(SQLiteSessionActorStore):
    """Keep pre-ownership store cases focused on their original contract."""

    async def enqueue(
        self,
        envelope: SessionEventEnvelope,
    ) -> EventEnqueueResult:
        ownership = self._database.agent_runtime_ownership.get(envelope.key)
        if ownership is None:
            ownership = self._database.agent_runtime_ownership.claim(
                envelope.key,
                AgentRuntimeOwnershipMode.ACTOR_V2,
                reason="session actor store integration test",
                legacy_session_id=(
                    f"legacy:{envelope.key.profile_id}:{envelope.key.session_id}"
                ),
            ).ownership
        return await super().enqueue(
            replace(envelope, ownership_generation=ownership.generation)
        )


def _make_store(tmp_path: Path) -> tuple[DatabaseManager, SQLiteSessionActorStore]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database, _OwnershipTestStore(
        database,
        lease_seconds=30.0,
        retry_delay_seconds=0.0,
        clock=lambda: 100.0,
        effect_contract_authority=_TEST_EFFECT_AUTHORITY,
    )


def _event(event_id: str, key: SessionKey) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="message_received",
        source="test",
        payload={"event_id": event_id},
        trace_id=f"trace:{event_id}",
    )


def _scheduled_journal(
    schedule: SessionReviewSchedule,
    *,
    schedule_event_id: str,
    previous_plan_id: str = "",
    operation_id: str = "",
    trace_id: str = "",
) -> SessionReviewScheduleEvent:
    return SessionReviewScheduleEvent(
        schedule_event_id=schedule_event_id,
        event_type="scheduled",
        plan_id=schedule.plan_id,
        previous_plan_id=previous_plan_id,
        trigger=schedule.trigger,
        outcome=schedule.outcome,
        source=schedule.source,
        requested_delay_seconds=schedule.requested_delay_seconds,
        applied_delay_seconds=schedule.applied_delay_seconds,
        reason=schedule.reason,
        fallback_reason=schedule.fallback_reason,
        model_execution_id=schedule.model_execution_id,
        prompt_signature=schedule.prompt_signature,
        expected_active_epoch=schedule.expected_active_epoch,
        expected_activity_generation=schedule.expected_activity_generation,
        committed_state_revision=schedule.committed_state_revision,
        operation_id=operation_id,
        trace_id=trace_id,
        metadata={
            "plan_revision": schedule.plan_revision,
            "schedule_outcome": {
                "active_reply_threshold": schedule.active_reply_threshold,
                "applied_delay_seconds": schedule.applied_delay_seconds,
                "fallback_reason": schedule.fallback_reason,
                "kind": schedule.outcome,
                "mention_sensitivity": schedule.mention_sensitivity or "normal",
                "model_execution_id": schedule.model_execution_id,
                "prompt_signature": schedule.prompt_signature,
                "reason": schedule.reason,
                "requested_delay_seconds": schedule.requested_delay_seconds,
                "source": schedule.source,
            },
        },
    )


@pytest.mark.asyncio
async def test_sqlite_session_store_isolates_same_session_across_profiles(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))

    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None
    aggregate_a = await store.load(key_a)
    aggregate_b = await store.load(key_b)
    await store.commit(
        claim_a,
        SessionTransition(
            aggregate=aggregate_a.advance(data={"owner": "profile-a"}),
            disposition="profile_a_applied",
            reason="test_a",
        ),
        expected_revision=aggregate_a.state_revision,
    )
    await store.commit(
        claim_b,
        SessionTransition(
            aggregate=aggregate_b.advance(data={"owner": "profile-b"}),
            disposition="profile_b_applied",
            reason="test_b",
        ),
        expected_revision=aggregate_b.state_revision,
    )

    assert (await store.load(key_a)).data == {"owner": "profile-a"}
    assert (await store.load(key_b)).data == {"owner": "profile-b"}
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, session_id, state_revision, event_sequence
            FROM agent_session_aggregates
            ORDER BY profile_id
            """
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("profile-a", "instance:group:room", 1, 1),
        ("profile-b", "instance:group:room", 1, 1),
    ]


@pytest.mark.asyncio
async def test_sqlite_session_store_enqueue_is_durably_idempotent(
    tmp_path: Path,
) -> None:
    _database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    envelope = _event("same-event", key)

    first = await store.enqueue(envelope)
    duplicate = await store.enqueue(envelope)
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    await store.commit(
        claim,
        SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="duplicate_test_applied",
        ),
        expected_revision=aggregate.state_revision,
    )
    completed_duplicate = await store.enqueue(envelope)

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.status == MailboxEventStatus.PENDING
    assert completed_duplicate.inserted is False
    assert completed_duplicate.status == MailboxEventStatus.COMPLETED
    with pytest.raises(MailboxEventConflict):
        await store.enqueue(
            SessionEventEnvelope(
                event_id="same-event",
                key=key,
                kind="message_received",
                payload={"event_id": "different-payload"},
            )
        )
    with pytest.raises(MailboxEventConflict):
        await store.enqueue(replace(envelope, source="different-source"))
    other_profile = await store.enqueue(
        _event("same-event", SessionKey("profile-b", key.session_id))
    )
    assert other_profile.inserted is True


@pytest.mark.asyncio
async def test_sqlite_session_store_recovers_only_expired_claims(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    now = [100.0]
    store = _OwnershipTestStore(
        database,
        lease_seconds=10.0,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("leased-event", key))
    first_claim = await store.claim_next(key, worker_id="worker-a")
    assert first_claim is not None

    assert await store.recover(key, worker_id="worker-b") == 0
    assert await store.claim_next(key, worker_id="worker-b") is None

    now[0] = 111.0
    assert await store.recover(key, worker_id="worker-b") == 1
    second_claim = await store.claim_next(key, worker_id="worker-b")
    assert second_claim is not None
    assert second_claim.attempt_count == 2


@pytest.mark.parametrize("record_kind", ["operation", "schedule"])
@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_durable_id_owner_changes(
    tmp_path: Path,
    record_kind: str,
) -> None:
    _database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))
    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None
    aggregate_a = await store.load(key_a)
    aggregate_b = await store.load(key_b)
    operations: tuple[SessionOperation, ...] = ()
    schedules: tuple[SessionReviewSchedule, ...] = ()
    if record_kind == "operation":
        operations = (
            SessionOperation(operation_id="shared-id", kind="review"),
        )
    else:
        schedules = (
            SessionReviewSchedule(
                plan_id="shared-id",
                plan_revision=1,
                applied_delay_seconds=120.0,
            ),
        )
    target_a = aggregate_a.advance(state_changed=False)
    target_b = aggregate_b.advance(state_changed=False)
    caused_plan_id = ""
    if schedules:
        target_a = aggregate_a.advance(
            current_plan_id="shared-id",
            review_plan_revision=1,
            review_plan={
                "plan_id": "shared-id",
                "plan_revision": 1,
                "applied_delay_seconds": 120.0,
            },
        )
        target_b = aggregate_b.advance(
            current_plan_id="shared-id",
            review_plan_revision=1,
            review_plan={
                "plan_id": "shared-id",
                "plan_revision": 1,
                "applied_delay_seconds": 120.0,
            },
        )
        caused_plan_id = "shared-id"
    await store.commit(
        claim_a,
        SessionTransition(
            aggregate=target_a,
            disposition="durable_record_created",
            caused_plan_id=caused_plan_id,
            operations=operations,
            review_schedules=schedules,
            review_schedule_events=(
                (
                    _scheduled_journal(
                        schedules[0],
                        schedule_event_id="schedule-event:profile-a",
                    ),
                )
                if schedules
                else ()
            ),
        ),
        expected_revision=aggregate_a.state_revision,
    )

    with pytest.raises(DurableRecordConflict):
        await store.commit(
            claim_b,
            SessionTransition(
                aggregate=target_b,
                disposition="durable_record_created",
                caused_plan_id=caused_plan_id,
                operations=operations,
                review_schedules=schedules,
                review_schedule_events=(
                    (
                        _scheduled_journal(
                            schedules[0],
                            schedule_event_id="schedule-event:profile-b",
                        ),
                    )
                    if schedules
                    else ()
                ),
            ),
            expected_revision=aggregate_b.state_revision,
        )

    restored_b = await store.load(key_b)
    assert restored_b.state_revision == 0
    assert restored_b.event_sequence == 0


@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_stale_aggregate_revision(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("stale-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = SessionTransition(
        aggregate=aggregate.advance(state="review"),
        disposition="entered_review",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state_revision = state_revision + 1
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )

    with pytest.raises(AggregateVersionConflict):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
        transitions = conn.execute("SELECT COUNT(*) FROM agent_state_transitions").fetchone()
    assert tuple(mailbox) == ("processing", claim.claim_id)
    assert transitions[0] == 0


@pytest.mark.asyncio
async def test_sqlite_session_store_rolls_back_all_atomic_commit_records(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("rollback-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = SessionTransition(
        aggregate=aggregate.advance(state="active_chat"),
        disposition="review_completed",
        caused_operation_id="operation:rollback",
        effects=(
            SessionEffect(
                effect_id="effect:rollback",
                kind="start_workflow",
                contract_signature=_TEST_EFFECT_CONTRACTS["start_workflow"].signature,
            ),
        ),
        operations=(
            SessionOperation(
                operation_id="operation:rollback",
                kind="review",
                status=SessionOperationStatus.RUNNING,
            ),
        ),
            review_schedule_events=(
                # Use a non-creation event so protocol validation passes, then
                # force SQL rollback after earlier records via duplicate identity.
                SessionReviewScheduleEvent(
                    schedule_event_id="schedule-event:rollback",
                    event_type="completed",
                ),
                SessionReviewScheduleEvent(
                    schedule_event_id="schedule-event:rollback",
                    event_type="completed",
                ),
        ),
        reason="review_complete",
    )

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    restored = await store.load(key)
    assert restored.state == "idle"
    assert restored.state_revision == 0
    assert restored.event_sequence == 0
    with database.connect() as conn:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "agent_session_operations",
                "agent_state_transitions",
                "agent_review_schedule_events",
                "agent_effect_outbox",
            )
        }
        mailbox_status = conn.execute(
            "SELECT status FROM agent_session_mailbox WHERE event_id = 'rollback-event'"
        ).fetchone()[0]
    assert counts == {
        "agent_session_operations": 0,
        "agent_state_transitions": 0,
        "agent_review_schedule_events": 0,
        "agent_effect_outbox": 0,
    }
    assert mailbox_status == "processing"


@pytest.mark.parametrize(
    "invalid_case",
    ("missing_fence", "wrong_signature", "unknown_version", "typed_fence"),
)
@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_invalid_effect_declarations_before_write(
    tmp_path: Path,
    invalid_case: str,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:invalid-effect")
    await store.enqueue(_event(f"invalid-effect:{invalid_case}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    current = await store.load(key)
    contract = builtin_effect_contract("run_idle_review_planning")
    payload: dict[str, object] = {
        "plan_id": "plan-a",
        "active_epoch": 0,
        "activity_generation": 0,
        "input_watermark": 10,
        "input_ledger_sequence": None,
        "completion_event_id": "completion-a",
        "failure_event_id": "failure-a",
        "source": "test",
        "trigger": "test",
    }
    version = contract.version
    signature = contract.signature
    if invalid_case == "missing_fence":
        payload.pop("plan_id")
    elif invalid_case == "wrong_signature":
        signature = "0" * 64
    elif invalid_case == "unknown_version":
        version += 100
    else:
        payload["input_watermark"] = 10.9
    effect = SessionEffect(
        effect_id=f"effect:{invalid_case}",
        kind=contract.effect_kind,
        contract_version=version,
        contract_signature=signature,
        payload=payload,
    )

    with pytest.raises(DurableRecordConflict):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=current.advance(data={"invalid_effect": invalid_case}),
                disposition="invalid_effect_declaration",
                effects=(effect,),
            ),
            expected_revision=current.state_revision,
        )

    assert await store.load(key) == current
    with database.connect() as conn:
        outbox_count = int(
            conn.execute("SELECT COUNT(*) FROM agent_effect_outbox").fetchone()[0]
        )
        transition_count = int(
            conn.execute("SELECT COUNT(*) FROM agent_state_transitions").fetchone()[0]
        )
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
    assert (outbox_count, transition_count) == (0, 0)
    assert tuple(mailbox) == ("processing", claim.claim_id)


@pytest.mark.parametrize(
    (
        "payload_handoff_operation_id",
        "payload_handoff_message_log_ids",
        "certificate_updates",
        "error_match",
    ),
    (
        ("another-review", [], {}, "verified review handoff operation"),
        ("review-handoff", [102], {}, "verified review handoff messages"),
        (
            "review-handoff",
            [],
            {"source_active_epoch": 1},
            "handoff certificate changed review proof",
        ),
    ),
)
@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_v3_bootstrap_effect_handoff_tampering(
    tmp_path: Path,
    payload_handoff_operation_id: str,
    payload_handoff_message_log_ids: list[int],
    certificate_updates: dict[str, object],
    error_match: str,
) -> None:
    """A v3 bootstrap effect cannot change its verified review handoff."""

    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:bootstrap-handoff")
    handoff_operation_id = "review-handoff"
    handoff_message_log_ids: list[int] = []
    handoff_watermark = 102
    handoff_ledger_sequence = 0
    handoff_certificate = {
        "version": 1,
        "review_operation_id": handoff_operation_id,
        "source_active_epoch": 0,
        "source_activity_generation": 0,
        "input_watermark": handoff_watermark,
        "input_ledger_sequence": handoff_ledger_sequence,
        "message_log_ids": handoff_message_log_ids,
        "review_consumption_id": "",
        "review_consumption_idempotency_key": "",
        "review_completion_event_id": "",
    }
    bootstrap_handoff_certificate = {
        **handoff_certificate,
        **certificate_updates,
    }

    await store.enqueue(_event("review-handoff-completed", key))
    review_claim = await store.claim_next(key, worker_id="review-worker")
    assert review_claim is not None
    initial = await store.load(key)
    await store.commit(
        review_claim,
        SessionTransition(
            aggregate=initial.advance(state_changed=False),
            disposition="review_handoff_completed",
            caused_operation_id=handoff_operation_id,
            operations=(
                SessionOperation(
                    operation_id=handoff_operation_id,
                    kind="review",
                    status=SessionOperationStatus.COMPLETED,
                    active_epoch=0,
                    activity_generation=0,
                    input_watermark=handoff_watermark,
                    input_ledger_sequence=handoff_ledger_sequence,
                    metadata={
                        "enter_active_chat": True,
                        "consumed_message_log_ids": handoff_message_log_ids,
                        "active_chat_handoff": handoff_certificate,
                    },
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )

    await store.enqueue(_event("bootstrap-effect-tampered", key))
    bootstrap_claim = await store.claim_next(key, worker_id="bootstrap-worker")
    assert bootstrap_claim is not None
    current = await store.load(key)
    bootstrap_operation_id = "bootstrap-operation"
    bootstrap_operation = SessionOperation(
        operation_id=bootstrap_operation_id,
        kind="active_chat_bootstrap",
        status=SessionOperationStatus.PENDING,
        input_watermark=handoff_watermark,
        input_ledger_sequence=handoff_ledger_sequence,
        metadata={
            "handoff_operation_id": handoff_operation_id,
                "handoff_message_log_ids": handoff_message_log_ids,
                "handoff_input_watermark": handoff_watermark,
                "handoff_input_ledger_sequence": handoff_ledger_sequence,
                "handoff_certificate": bootstrap_handoff_certificate,
        },
    )
    contract = builtin_effect_contract("run_active_chat_bootstrap")
    bootstrap_effect = SessionEffect(
        effect_id="bootstrap-effect",
        kind=contract.effect_kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        idempotency_key="bootstrap-effect",
        operation_id=bootstrap_operation_id,
        payload={
            "plan_id": "plan-a",
            "active_epoch": 0,
            "activity_generation": 0,
            "input_watermark": handoff_watermark,
            "input_ledger_sequence": handoff_ledger_sequence,
            "completion_event_id": "bootstrap-completed",
            "failure_event_id": "bootstrap-failed",
            "handoff_operation_id": payload_handoff_operation_id,
            "handoff_message_log_ids": payload_handoff_message_log_ids,
        },
    )
    target = current.advance(
        data={
            "operation_fences": {
                bootstrap_operation_id: {
                    "input_watermark": handoff_watermark,
                    "input_ledger_sequence": handoff_ledger_sequence,
                }
            }
        }
    )

    with pytest.raises(DurableRecordConflict, match=error_match):
        await store.commit(
            bootstrap_claim,
            SessionTransition(
                aggregate=target,
                disposition="bootstrap_effect_tampered",
                caused_operation_id=bootstrap_operation_id,
                effects=(bootstrap_effect,),
                operations=(bootstrap_operation,),
            ),
            expected_revision=current.state_revision,
        )

    assert await store.load(key) == current
    with database.connect() as conn:
        review_operation = conn.execute(
            """
            SELECT status, metadata_json
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (handoff_operation_id,),
        ).fetchone()
        bootstrap_operation_row = conn.execute(
            """
            SELECT operation_id FROM agent_session_operations WHERE operation_id = ?
            """,
            (bootstrap_operation_id,),
        ).fetchone()
        bootstrap_effect_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM agent_effect_outbox WHERE effect_id = ?",
                (bootstrap_effect.effect_id,),
            ).fetchone()[0]
        )
        mailbox = conn.execute(
            """
            SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?
            """,
            (bootstrap_claim.envelope.event_id,),
        ).fetchone()
    assert review_operation is not None
    assert review_operation["status"] == SessionOperationStatus.COMPLETED.value
    assert json.loads(str(review_operation["metadata_json"])) == {
        "active_chat_handoff": handoff_certificate,
        "consumed_message_log_ids": handoff_message_log_ids,
        "enter_active_chat": True,
    }
    assert bootstrap_operation_row is None
    assert bootstrap_effect_count == 0
    assert tuple(mailbox) == ("processing", bootstrap_claim.claim_id)


@pytest.mark.parametrize(
    ("tamper_target", "field_name", "tampered_value", "error_match"),
    (
        (
            "payload",
            "message_log_ids",
            [102, 101],
            "ordered operation-fence message selection",
        ),
        (
            "payload",
            "round_schedule_id",
            "other-round-schedule",
            "operation-fence round_schedule_id",
        ),
        (
            "payload",
            "active_chat_interest_value",
            20,
            "operation-fence active_chat_interest_value",
        ),
        (
            "payload",
            "bootstrap_disposition",
            "engaged",
            "operation-fence bootstrap_disposition",
        ),
        (
            "payload_missing",
            "bootstrap_disposition",
            None,
            "missing declared outcome fence fields",
        ),
        (
            "operation_fence_missing",
            "bootstrap_disposition",
            None,
            "operation fence bootstrap_disposition must be non-empty JSON text",
        ),
    ),
)
@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_v3_round_effect_fence_tampering(
    tmp_path: Path,
    tamper_target: str,
    field_name: str,
    tampered_value: object,
    error_match: str,
) -> None:
    """A v3 round effect cannot change its aggregate operation fence."""

    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:round-fence")
    operation_id = "round-operation"
    input_watermark = 102
    input_ledger_sequence = 0
    operation_fence: dict[str, object] = {
        "input_watermark": input_watermark,
        "input_ledger_sequence": input_ledger_sequence,
        "message_log_ids": [101, 102],
        "round_schedule_id": "round-schedule-a",
        "active_chat_interest_value": 20.0,
        "bootstrap_disposition": "watch",
    }
    payload: dict[str, object] = {
        "plan_id": "plan-a",
        "active_epoch": 0,
        "activity_generation": 0,
        "input_watermark": input_watermark,
        "input_ledger_sequence": input_ledger_sequence,
        "completion_event_id": "round-completed",
        "failure_event_id": "round-failed",
        "message_log_ids": [101, 102],
        "round_schedule_id": "round-schedule-a",
        "active_chat_interest_value": 20.0,
        "bootstrap_disposition": "watch",
    }
    if tamper_target == "payload":
        payload[field_name] = tampered_value
    elif tamper_target == "payload_missing":
        payload.pop(field_name)
    elif tamper_target == "operation_fence_missing":
        operation_fence.pop(field_name)
    else:
        raise AssertionError(f"unsupported tamper target: {tamper_target}")

    await store.enqueue(_event(f"round-fence:{tamper_target}:{field_name}", key))
    claim = await store.claim_next(key, worker_id="round-worker")
    assert claim is not None
    current = await store.load(key)
    target = current.advance(
        data={"operation_fences": {operation_id: operation_fence}}
    )
    operation = SessionOperation(
        operation_id=operation_id,
        kind="active_chat_round",
        status=SessionOperationStatus.PENDING,
        input_watermark=input_watermark,
        input_ledger_sequence=input_ledger_sequence,
    )
    contract = builtin_effect_contract("run_active_chat_round")
    assert contract.version == 3
    effect = SessionEffect(
        effect_id="round-effect",
        kind=contract.effect_kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        idempotency_key="round-effect",
        operation_id=operation_id,
        payload=payload,
    )

    with pytest.raises(DurableRecordConflict, match=error_match):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="round_effect_tampered",
                caused_operation_id=operation_id,
                effects=(effect,),
                operations=(operation,),
            ),
            expected_revision=current.state_revision,
        )

    assert await store.load(key) == current
    with database.connect() as conn:
        operation_row = conn.execute(
            "SELECT operation_id FROM agent_session_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        effect_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM agent_effect_outbox WHERE effect_id = ?",
                (effect.effect_id,),
            ).fetchone()[0]
        )
        mailbox = conn.execute(
            """
            SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?
            """,
            (claim.envelope.event_id,),
        ).fetchone()
    assert operation_row is None
    assert effect_count == 0
    assert tuple(mailbox) == ("processing", claim.claim_id)


@pytest.mark.asyncio
async def test_sqlite_session_store_commits_operation_schedule_journals_and_effect(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("commit-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        state="idle",
        current_plan_id="plan-1",
        review_plan_revision=1,
        review_plan={
            "plan_id": "plan-1",
            "plan_revision": 1,
            "trigger": "active_chat_decay_exit",
            "kind": "planned",
            "source": "llm",
            "requested_delay_seconds": 120.0,
            "applied_delay_seconds": 120.0,
            "reason": "topic_settled",
            "model_execution_id": "execution-1",
        },
        idle_planning_operation_id="operation-1",
    )
    schedule = SessionReviewSchedule(
        plan_id="plan-1",
        plan_revision=1,
        trigger="active_chat_decay_exit",
        outcome="planned",
        source="llm",
        requested_delay_seconds=120.0,
        applied_delay_seconds=120.0,
        reason="topic_settled",
        model_execution_id="execution-1",
    )

    committed = await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="idle_review_planned",
            caused_operation_id="operation-1",
            caused_plan_id="plan-1",
            effects=(
                SessionEffect(
                    effect_id="effect-1",
                    kind="schedule_review_timer",
                    contract_signature=_TEST_EFFECT_CONTRACTS[
                        "schedule_review_timer"
                    ].signature,
                    operation_id="operation-1",
                ),
            ),
            operations=(
                SessionOperation(
                    operation_id="operation-1",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.COMPLETED,
                    finished_at=100.0,
                ),
            ),
            review_schedules=(schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    schedule,
                    schedule_event_id="schedule-event-1",
                    operation_id="operation-1",
                ),
            ),
            result={"planning": "accepted"},
            reason="active_chat_decay_exit",
        ),
        expected_revision=aggregate.state_revision,
    )

    assert committed.state_revision == 1
    assert committed.event_sequence == 1
    assert committed.review_plan["scheduled_from"] == 100.0
    assert committed.review_plan["next_review_at"] == 220.0
    assert committed.review_plan["applied_delay_seconds"] == 120.0
    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = 'commit-event'"
        ).fetchone()
        operation = conn.execute(
            "SELECT kind, status FROM agent_session_operations WHERE operation_id = 'operation-1'"
        ).fetchone()
        schedule = conn.execute(
            "SELECT source, next_review_at FROM agent_review_schedules WHERE plan_id = 'plan-1'"
        ).fetchone()
        transition = conn.execute(
            """
            SELECT trigger, disposition, operation_id, plan_id
            FROM agent_state_transitions
            """
        ).fetchone()
        schedule_event = conn.execute(
            """
            SELECT event_type, model_execution_id, scheduled_from, next_review_at
            FROM agent_review_schedule_events
            """
        ).fetchone()
        effect = conn.execute(
            "SELECT kind, status FROM agent_effect_outbox WHERE effect_id = 'effect-1'"
        ).fetchone()
    assert tuple(mailbox) == ("completed", "")
    assert tuple(operation) == ("idle_review_planning", "completed")
    assert tuple(schedule) == ("llm", 220.0)
    assert tuple(transition) == (
        "active_chat_decay_exit",
        "idle_review_planned",
        "operation-1",
        "plan-1",
    )
    assert tuple(schedule_event) == ("scheduled", "execution-1", 100.0, 220.0)
    assert tuple(effect) == ("schedule_review_timer", "pending")
    assert committed.review_plan["plan_id"] == "plan-1"
    assert committed.review_plan["plan_revision"] == 1
    assert committed.review_plan["scheduled_from"] == 100.0
    assert committed.review_plan["next_review_at"] == 220.0


@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_plan_advance_without_schedule(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("orphan-plan-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        current_plan_id="orphan-plan",
        review_plan_revision=1,
        review_plan={"plan_id": "orphan-plan", "plan_revision": 1},
    )

    with pytest.raises(
        DurableRecordConflict,
        match="requires exactly one schedule",
    ):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="invalid_plan_advance",
                caused_plan_id="orphan-plan",
            ),
            expected_revision=aggregate.state_revision,
        )

    restored = await store.load(key)
    assert restored.current_plan_id == ""
    assert restored.review_plan_revision == 0
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT status, claim_id, lease_owner FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, claim.envelope.event_id),
        ).fetchone()
        schedule_count = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_review_schedules"
        ).fetchone()
        journal_count = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_review_schedule_events"
        ).fetchone()
        transition_count = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_state_transitions"
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == ("processing", claim.claim_id, claim.worker_id)
    assert schedule_count is not None
    assert int(schedule_count["count"]) == 0
    assert journal_count is not None
    assert int(journal_count["count"]) == 0
    assert transition_count is not None
    assert int(transition_count["count"]) == 0


@pytest.mark.parametrize(
    ("journal_case", "error_match"),
    (
        ("missing", "exactly one scheduled journal"),
        ("duplicate", "exactly one scheduled journal"),
        ("wrong_event_type", "event_type scheduled"),
        ("wrong_plan", "does not match aggregate current_plan_id"),
        ("wrong_previous_plan", "changed previous_plan_id"),
        ("wrong_revision", "revision does not match"),
        ("wrong_semantics", "does not match schedule semantics"),
        ("caller_clock", "caller-owned clock fields"),
    ),
)
@pytest.mark.asyncio
async def test_new_review_plan_requires_one_coherent_scheduled_journal(
    tmp_path: Path,
    journal_case: str,
    error_match: str,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:journal")
    await store.enqueue(_event(f"journal:{journal_case}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    current = await store.load(key)
    schedule = SessionReviewSchedule(
        plan_id="journal-plan",
        plan_revision=1,
        applied_delay_seconds=30.0,
        trigger="review_completed",
        outcome="planned",
        source="unit-policy",
        reason="topic settled",
    )
    valid = _scheduled_journal(
        schedule,
        schedule_event_id="journal-plan-scheduled",
    )
    if journal_case == "missing":
        journals: tuple[SessionReviewScheduleEvent, ...] = ()
    elif journal_case == "duplicate":
        journals = (
            valid,
            replace(valid, schedule_event_id="journal-plan-scheduled-duplicate"),
        )
    elif journal_case == "wrong_event_type":
        journals = (replace(valid, event_type="planned"),)
    elif journal_case == "wrong_plan":
        journals = (replace(valid, plan_id="other-plan"),)
    elif journal_case == "wrong_previous_plan":
        journals = (replace(valid, previous_plan_id="other-plan"),)
    elif journal_case == "wrong_revision":
        journals = (replace(valid, metadata={"plan_revision": 2}),)
    elif journal_case == "wrong_semantics":
        journals = (replace(valid, trigger="manual_review"),)
    else:
        journals = (replace(valid, scheduled_from=1.0),)

    target = current.advance(
        current_plan_id="journal-plan",
        review_plan_revision=1,
        review_plan={
            "plan_id": "journal-plan",
            "plan_revision": 1,
            "applied_delay_seconds": 30.0,
            "trigger": "review_completed",
            "kind": "planned",
            "source": "unit-policy",
            "reason": "topic settled",
        },
    )
    with pytest.raises(DurableRecordConflict, match=error_match):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="invalid_scheduled_journal",
                caused_plan_id="journal-plan",
                review_schedules=(schedule,),
                review_schedule_events=journals,
            ),
            expected_revision=current.state_revision,
        )

    assert await store.load(key) == current
    with database.connect() as conn:
        counts = tuple(
            int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in (
                "agent_review_schedules",
                "agent_review_schedule_events",
                "agent_state_transitions",
            )
        )
    assert counts == (0, 0, 0)


@pytest.mark.parametrize(
    "status",
    (
        ReviewScheduleStatus.COMPLETED,
        ReviewScheduleStatus.FAILED,
        ReviewScheduleStatus.SUPERSEDED,
    ),
)
@pytest.mark.asyncio
async def test_sqlite_store_rejects_terminal_schedule_for_new_current_plan(
    tmp_path: Path,
    status: ReviewScheduleStatus,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event(f"terminal-new-plan:{status.value}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    current = await store.load(key)
    target = current.advance(
        current_plan_id="terminal-new-plan",
        review_plan_revision=1,
        review_plan={
            "plan_id": "terminal-new-plan",
            "plan_revision": 1,
            "applied_delay_seconds": 30.0,
        },
    )
    schedule = SessionReviewSchedule(
        plan_id="terminal-new-plan",
        plan_revision=1,
        applied_delay_seconds=30.0,
        status=status,
    )

    with pytest.raises(
        DurableRecordConflict,
        match="must start scheduled",
    ):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="invalid_terminal_new_plan",
                caused_plan_id="terminal-new-plan",
                review_schedules=(schedule,),
                review_schedule_events=(
                    _scheduled_journal(
                        schedule,
                        schedule_event_id=f"terminal-new-plan:{status.value}:scheduled",
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )

    restored = await store.load(key)
    assert restored == current
    with database.connect() as conn:
        schedule_count = int(
            conn.execute("SELECT COUNT(*) FROM agent_review_schedules").fetchone()[0]
        )
        transition_count = int(
            conn.execute("SELECT COUNT(*) FROM agent_state_transitions").fetchone()[0]
        )
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
    assert schedule_count == 0
    assert transition_count == 0
    assert tuple(mailbox) == ("processing", claim.claim_id)


@pytest.mark.parametrize(
    ("target_plan_id", "target_revision", "error_match"),
    (
        ("plan-b", 99, "revision must advance by exactly one"),
        ("plan-b", 1, "id and revision must advance together"),
        ("plan-a", 2, "id and revision must advance together"),
    ),
)
@pytest.mark.asyncio
async def test_sqlite_store_rejects_unsynchronized_plan_fence_before_writes(
    tmp_path: Path,
    target_plan_id: str,
    target_revision: int,
    error_match: str,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("baseline-plan", key))
    baseline_claim = await store.claim_next(key, worker_id="worker")
    assert baseline_claim is not None
    initial = await store.load(key)
    baseline_schedule = SessionReviewSchedule(
        plan_id="plan-a",
        plan_revision=1,
        applied_delay_seconds=30.0,
    )
    baseline = await store.commit(
        baseline_claim,
        SessionTransition(
            aggregate=initial.advance(
                current_plan_id="plan-a",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "plan-a",
                    "plan_revision": 1,
                    "applied_delay_seconds": 30.0,
                },
            ),
            disposition="baseline_plan_created",
            caused_plan_id="plan-a",
            review_schedules=(baseline_schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    baseline_schedule,
                    schedule_event_id="baseline-plan-scheduled",
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    await store.enqueue(
        _event(f"invalid-plan:{target_plan_id}:{target_revision}", key)
    )
    invalid_claim = await store.claim_next(key, worker_id="worker")
    assert invalid_claim is not None
    malformed = replace(
        baseline,
        current_plan_id=target_plan_id,
        review_plan_revision=target_revision,
        review_plan={
            "plan_id": target_plan_id,
            "plan_revision": target_revision,
            "applied_delay_seconds": 30.0,
        },
        state_revision=baseline.state_revision + 1,
        event_sequence=baseline.event_sequence + 1,
    )

    with pytest.raises(DurableRecordConflict, match=error_match):
        await store.commit(
            invalid_claim,
            SessionTransition(
                aggregate=malformed,
                disposition="invalid_plan_fence",
                caused_plan_id=target_plan_id,
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id=target_plan_id,
                        plan_revision=target_revision,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
            expected_revision=baseline.state_revision,
        )

    restored = await store.load(key)
    assert restored == baseline
    with database.connect() as conn:
        schedules = conn.execute(
            "SELECT plan_id, plan_revision FROM agent_review_schedules"
        ).fetchall()
        invalid_mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (invalid_claim.envelope.event_id,),
        ).fetchone()
        transition_count = conn.execute(
            "SELECT COUNT(*) FROM agent_state_transitions"
        ).fetchone()[0]
    assert [tuple(row) for row in schedules] == [("plan-a", 1)]
    assert tuple(invalid_mailbox) == ("processing", invalid_claim.claim_id)
    assert int(transition_count) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "review_plan",
    (
        {"plan_id": "plan-b", "plan_revision": 1},
        {"plan_id": " plan-a ", "plan_revision": 1},
        {"plan_id": "plan-a", "plan_revision": 99},
    ),
)
async def test_sqlite_session_store_rejects_split_brain_review_plan_identity(
    tmp_path: Path,
    review_plan: dict[str, object],
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("split-brain-plan-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        current_plan_id="plan-a",
        review_plan_revision=1,
        review_plan=review_plan,
    )
    schedule = SessionReviewSchedule(
        plan_id="plan-a",
        plan_revision=1,
        applied_delay_seconds=30.0,
    )

    with pytest.raises(
        DurableRecordConflict,
        match="review plan payload does not match",
    ):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="invalid_plan_identity",
                caused_plan_id="plan-a",
                review_schedules=(schedule,),
                review_schedule_events=(
                    _scheduled_journal(
                        schedule,
                        schedule_event_id="split-brain-plan-scheduled",
                    ),
                ),
            ),
            expected_revision=aggregate.state_revision,
        )

    restored = await store.load(key)
    assert restored.current_plan_id == ""
    assert restored.review_plan_revision == 0
    assert restored.review_plan == {}
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT status, claim_id, lease_owner FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, claim.envelope.event_id),
        ).fetchone()
        schedule_count = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_review_schedules"
        ).fetchone()
        transition_count = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_state_transitions"
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == ("processing", claim.claim_id, claim.worker_id)
    assert schedule_count is not None
    assert int(schedule_count["count"]) == 0
    assert transition_count is not None
    assert int(transition_count["count"]) == 0


@pytest.mark.asyncio
async def test_existing_plan_status_update_cannot_change_plan_payload_identity(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("create-existing-plan", key))
    create_claim = await store.claim_next(key, worker_id="worker")
    assert create_claim is not None
    initial = await store.load(key)
    initial_schedule = SessionReviewSchedule(
        plan_id="plan-a",
        plan_revision=1,
        applied_delay_seconds=30.0,
    )
    planned = await store.commit(
        create_claim,
        SessionTransition(
            aggregate=initial.advance(
                current_plan_id="plan-a",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "plan-a",
                    "plan_revision": 1,
                    "applied_delay_seconds": 30.0,
                },
            ),
            disposition="review_planned",
            caused_plan_id="plan-a",
            review_schedules=(initial_schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    initial_schedule,
                    schedule_event_id="existing-plan-scheduled",
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    await store.enqueue(_event("complete-existing-plan", key))
    complete_claim = await store.claim_next(key, worker_id="worker")
    assert complete_claim is not None
    split_brain = planned.advance(
        review_plan={**planned.review_plan, "plan_revision": 99},
    )

    with pytest.raises(
        DurableRecordConflict,
        match="existing review plan payload is immutable",
    ):
        await store.commit(
            complete_claim,
            SessionTransition(
                aggregate=split_brain,
                disposition="review_completed",
                caused_plan_id="plan-a",
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id="plan-a",
                        plan_revision=1,
                        applied_delay_seconds=30.0,
                        status=ReviewScheduleStatus.COMPLETED,
                    ),
                ),
            ),
            expected_revision=planned.state_revision,
        )

    restored = await store.load(key)
    assert restored.review_plan["plan_revision"] == 1
    with database.connect() as conn:
        schedule = conn.execute(
            "SELECT status FROM agent_review_schedules WHERE plan_id = 'plan-a'"
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT status, claim_id FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, complete_claim.envelope.event_id),
        ).fetchone()
    assert schedule is not None
    assert str(schedule["status"]) == ReviewScheduleStatus.SCHEDULED
    assert mailbox is not None
    assert tuple(mailbox) == ("processing", complete_claim.claim_id)


@pytest.mark.asyncio
async def test_same_plan_cannot_persist_canonicalized_scheduled_journal(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:journal-alias")
    await store.enqueue(_event("create-journal-alias-plan", key))
    create_claim = await store.claim_next(key, worker_id="worker")
    assert create_claim is not None
    initial = await store.load(key)
    schedule = SessionReviewSchedule(
        plan_id="plan-a",
        plan_revision=1,
        applied_delay_seconds=30.0,
    )
    planned = await store.commit(
        create_claim,
        SessionTransition(
            aggregate=initial.advance(
                current_plan_id="plan-a",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "plan-a",
                    "plan_revision": 1,
                    "applied_delay_seconds": 30.0,
                },
            ),
            disposition="review_planned",
            caused_plan_id="plan-a",
            review_schedules=(schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    schedule,
                    schedule_event_id="plan-a-scheduled",
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    await store.enqueue(_event("forge-journal-alias", key))
    forged_claim = await store.claim_next(key, worker_id="worker")
    assert forged_claim is not None

    with pytest.raises(DurableRecordConflict, match="surrounding whitespace"):
        await store.commit(
            forged_claim,
            SessionTransition(
                aggregate=planned.advance(state_changed=False),
                disposition="forged_schedule_evidence",
                review_schedule_events=(
                    SessionReviewScheduleEvent(
                        schedule_event_id="forged-plan-a-scheduled",
                        event_type=" scheduled ",
                        plan_id="plan-a",
                        outcome="forged",
                        applied_delay_seconds=999.0,
                        metadata={
                            "schedule_outcome": {"kind": ["invalid"]},
                        },
                    ),
                ),
            ),
            expected_revision=planned.state_revision,
        )

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT schedule_event_id, event_type
            FROM agent_review_schedule_events
            ORDER BY schedule_event_seq
            """
        ).fetchall()
        mailbox = conn.execute(
            """
            SELECT status, claim_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, forged_claim.envelope.event_id),
        ).fetchone()
    assert [tuple(row) for row in rows] == [("plan-a-scheduled", "scheduled")]
    assert tuple(mailbox) == ("processing", forged_claim.claim_id)


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    (
        ("applied_delay_seconds", 31.0),
        ("trigger", "manual_review"),
        ("kind", "defaulted"),
        ("source", "other-policy"),
        ("requested_delay_seconds", None),
        ("reason", "different reason"),
        ("fallback_reason", "different fallback"),
        ("mention_sensitivity", "high"),
        ("active_reply_threshold", {"mention_score": 0.9}),
        ("model_execution_id", "execution-b"),
        ("prompt_signature", "prompt-b"),
        ("expected_active_epoch", 1),
        ("expected_activity_generation", 1),
        ("committed_state_revision", 0),
        ("next_review_at", 999.0),
    ),
)
@pytest.mark.asyncio
async def test_new_review_plan_semantic_mismatch_rolls_back_every_record(
    tmp_path: Path,
    field_name: str,
    changed_value: object,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event(f"semantic-mismatch:{field_name}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    current = await store.load(key)
    review_plan: dict[str, object] = {
        "plan_id": "plan-semantic",
        "plan_revision": 1,
        "applied_delay_seconds": 30.0,
        "trigger": "review_completed",
        "kind": "planned",
        "source": "review-policy",
        "requested_delay_seconds": 45.0,
        "reason": "topic settled",
        "fallback_reason": "",
        "mention_sensitivity": "normal",
        "active_reply_threshold": {"mention_score": 0.75},
        "model_execution_id": "execution-a",
        "prompt_signature": "prompt-a",
        "expected_active_epoch": 0,
        "expected_activity_generation": 0,
        "committed_state_revision": 1,
    }
    review_plan[field_name] = changed_value
    target = current.advance(
        current_plan_id="plan-semantic",
        review_plan_revision=1,
        review_plan=review_plan,
    )
    schedule = SessionReviewSchedule(
        plan_id="plan-semantic",
        plan_revision=1,
        applied_delay_seconds=30.0,
        trigger="review_completed",
        outcome="planned",
        source="review-policy",
        requested_delay_seconds=45.0,
        reason="topic settled",
        fallback_reason="",
        mention_sensitivity="normal",
        active_reply_threshold={"mention_score": 0.75},
        model_execution_id="execution-a",
        prompt_signature="prompt-a",
        expected_active_epoch=0,
        expected_activity_generation=0,
        committed_state_revision=1,
    )

    with pytest.raises(DurableRecordConflict):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="semantic_mismatch",
                caused_plan_id="plan-semantic",
                review_schedules=(schedule,),
                review_schedule_events=(
                    _scheduled_journal(
                        schedule,
                        schedule_event_id=f"semantic-event:{field_name}",
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )

    restored = await store.load(key)
    assert restored.current_plan_id == ""
    assert restored.review_plan_revision == 0
    assert restored.review_plan == {}
    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
        schedule_count = conn.execute(
            "SELECT COUNT(*) FROM agent_review_schedules"
        ).fetchone()[0]
        schedule_event_count = conn.execute(
            "SELECT COUNT(*) FROM agent_review_schedule_events"
        ).fetchone()[0]
        transition_count = conn.execute(
            "SELECT COUNT(*) FROM agent_state_transitions"
        ).fetchone()[0]
    assert tuple(mailbox) == ("processing", claim.claim_id)
    assert (schedule_count, schedule_event_count, transition_count) == (0, 0, 0)


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    (
        ("reason", "changed after commit"),
        ("active_reply_threshold", {"mention_score": 1.0}),
        ("applied_delay_seconds", 30),
    ),
)
@pytest.mark.asyncio
async def test_existing_plan_status_only_update_requires_canonical_payload(
    tmp_path: Path,
    field_name: str,
    changed_value: object,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("canonical-plan-created", key))
    create_claim = await store.claim_next(key, worker_id="worker")
    assert create_claim is not None
    initial = await store.load(key)
    plan = {
        "plan_id": "canonical-plan",
        "plan_revision": 1,
        "applied_delay_seconds": 30.0,
        "trigger": "review_completed",
        "kind": "planned",
        "reason": "stable reason",
        "active_reply_threshold": {"mention_score": 0.75},
    }
    initial_schedule = SessionReviewSchedule(
        plan_id="canonical-plan",
        plan_revision=1,
        applied_delay_seconds=30.0,
        trigger="review_completed",
        outcome="planned",
        reason="stable reason",
        active_reply_threshold={"mention_score": 0.75},
    )
    planned = await store.commit(
        create_claim,
        SessionTransition(
            aggregate=initial.advance(
                current_plan_id="canonical-plan",
                review_plan_revision=1,
                review_plan=plan,
            ),
            disposition="canonical_plan_created",
            caused_plan_id="canonical-plan",
            review_schedules=(initial_schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    initial_schedule,
                    schedule_event_id="canonical-plan-scheduled",
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    with database.connect() as conn:
        persisted_plan_json = str(
            conn.execute(
                "SELECT review_plan_json FROM agent_session_aggregates"
            ).fetchone()[0]
        )

    await store.enqueue(_event(f"canonical-plan-updated:{field_name}", key))
    update_claim = await store.claim_next(key, worker_id="worker")
    assert update_claim is not None
    changed_plan = dict(planned.review_plan)
    changed_plan[field_name] = changed_value
    target = planned.advance(review_plan=changed_plan)

    with pytest.raises(
        DurableRecordConflict,
        match="existing review plan payload is immutable",
    ):
        await store.commit(
            update_claim,
            SessionTransition(
                aggregate=target,
                disposition="canonical_plan_status_update",
                caused_plan_id="canonical-plan",
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id="canonical-plan",
                        plan_revision=1,
                        applied_delay_seconds=30.0,
                        status=ReviewScheduleStatus.COMPLETED,
                    ),
                ),
            ),
            expected_revision=planned.state_revision,
        )

    with database.connect() as conn:
        aggregate_row = conn.execute(
            "SELECT review_plan_json FROM agent_session_aggregates"
        ).fetchone()
        schedule_row = conn.execute(
            "SELECT status FROM agent_review_schedules WHERE plan_id = ?",
            ("canonical-plan",),
        ).fetchone()
        mailbox_row = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (update_claim.envelope.event_id,),
        ).fetchone()
    assert str(aggregate_row[0]) == persisted_plan_json
    assert str(schedule_row["status"]) == ReviewScheduleStatus.SCHEDULED
    assert tuple(mailbox_row) == ("processing", update_claim.claim_id)


@pytest.mark.asyncio
async def test_existing_plan_status_update_preserves_legacy_canonical_numbers(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("legacy-canonical-plan-created", key))
    create_claim = await store.claim_next(key, worker_id="worker")
    assert create_claim is not None
    initial = await store.load(key)
    initial_schedule = SessionReviewSchedule(
        plan_id="legacy-canonical-plan",
        plan_revision=1,
        applied_delay_seconds=30.0,
    )
    planned = await store.commit(
        create_claim,
        SessionTransition(
            aggregate=initial.advance(
                current_plan_id="legacy-canonical-plan",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "legacy-canonical-plan",
                    "plan_revision": 1,
                    "applied_delay_seconds": 30.0,
                },
            ),
            disposition="legacy_canonical_plan_created",
            caused_plan_id="legacy-canonical-plan",
            review_schedules=(initial_schedule,),
            review_schedule_events=(
                _scheduled_journal(
                    initial_schedule,
                    schedule_event_id="legacy-canonical-plan-scheduled",
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    legacy_plan = dict(planned.review_plan)
    legacy_plan["applied_delay_seconds"] = 30
    legacy_plan_json = json.dumps(
        legacy_plan,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET review_plan_json = ?
            WHERE profile_id = ? AND session_id = ?
            """,
            (legacy_plan_json, key.profile_id, key.session_id),
        )

    legacy = await store.load(key)
    assert type(legacy.review_plan["applied_delay_seconds"]) is int
    await store.enqueue(_event("legacy-canonical-plan-completed", key))
    update_claim = await store.claim_next(key, worker_id="worker")
    assert update_claim is not None
    committed = await store.commit(
        update_claim,
        SessionTransition(
            aggregate=legacy.advance(state_changed=False),
            disposition="legacy_canonical_plan_completed",
            caused_plan_id="legacy-canonical-plan",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="legacy-canonical-plan",
                    plan_revision=1,
                    applied_delay_seconds=30.0,
                    status=ReviewScheduleStatus.COMPLETED,
                ),
            ),
        ),
        expected_revision=legacy.state_revision,
    )

    assert type(committed.review_plan["applied_delay_seconds"]) is int
    with database.connect() as conn:
        aggregate_json = str(
            conn.execute(
                "SELECT review_plan_json FROM agent_session_aggregates"
            ).fetchone()[0]
        )
        schedule_status = str(
            conn.execute(
                "SELECT status FROM agent_review_schedules"
            ).fetchone()[0]
        )
    assert aggregate_json == legacy_plan_json
    assert schedule_status == ReviewScheduleStatus.COMPLETED


@pytest.mark.asyncio
async def test_sqlite_session_store_fails_only_the_owned_mailbox_event(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))
    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None

    await store.fail(claim_a, error="invalid_transition")

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, status, claim_id, lease_owner, last_error
            FROM agent_session_mailbox
            WHERE session_id = ? AND event_id = ?
            ORDER BY profile_id
            """,
            (key_a.session_id, claim_a.envelope.event_id),
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("profile-a", "failed", "", "", "invalid_transition"),
        ("profile-b", "processing", claim_b.claim_id, "worker-b", ""),
    ]
    assert await store.claim_next(key_a, worker_id="worker-c") is None
    with pytest.raises(MailboxLeaseConflict, match="not owned"):
        await store.fail(claim_a, error="duplicate")

    failed_aggregate = await store.load(key_a)
    assert failed_aggregate.state_revision == 0
    assert failed_aggregate.event_sequence == 1
    await store.enqueue(_event("healthy-event", key_a))
    healthy_claim = await store.claim_next(key_a, worker_id="worker-c")
    assert healthy_claim is not None
    await store.commit(
        healthy_claim,
        SessionTransition(
            aggregate=failed_aggregate.advance(state_changed=False),
            disposition="healthy_noop",
        ),
        expected_revision=failed_aggregate.state_revision,
    )
    with database.connect() as conn:
        journal = conn.execute(
            """
            SELECT event_id, disposition, state_revision, event_sequence
            FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ?
            ORDER BY event_sequence
            """,
            (key_a.profile_id, key_a.session_id),
        ).fetchall()
    assert [tuple(row) for row in journal] == [
        ("same-event", "failed", 0, 1),
        ("healthy-event", "healthy_noop", 0, 2),
    ]


@pytest.mark.asyncio
async def test_sqlite_session_store_preserves_zero_operation_fences_and_rejects_regression(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("operation-start", key))
    first_claim = await store.claim_next(key, worker_id="worker")
    assert first_claim is not None
    aggregate = await store.load(key)
    await store.commit(
        first_claim,
        SessionTransition(
            aggregate=aggregate.advance(active_epoch=3, activity_generation=4),
            disposition="operation_started",
            caused_operation_id="operation-1",
            operations=(
                SessionOperation(
                    operation_id="operation-1",
                    kind="review",
                    status=SessionOperationStatus.RUNNING,
                    state_revision=0,
                    active_epoch=0,
                    activity_generation=0,
                    started_at=0.0,
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, state_revision, active_epoch, activity_generation, started_at
            FROM agent_session_operations WHERE operation_id = 'operation-1'
            """
        ).fetchone()
    assert tuple(operation) == ("running", 0, 0, 0, 0.0)

    await store.enqueue(_event("operation-regression", key))
    second_claim = await store.claim_next(key, worker_id="worker")
    assert second_claim is not None
    current = await store.load(key)
    with pytest.raises(DurableRecordConflict, match="cannot move backwards"):
        await store.commit(
            second_claim,
            SessionTransition(
                aggregate=current.advance(state_changed=False),
                disposition="operation_regressed",
                caused_operation_id="operation-1",
                operations=(
                    SessionOperation(
                        operation_id="operation-1",
                        kind="review",
                        status=SessionOperationStatus.PENDING,
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )
    restored = await store.load(key)
    assert restored.state_revision == current.state_revision
    assert restored.event_sequence == current.event_sequence


@pytest.mark.parametrize("revision_delta", [0, 1])
@pytest.mark.asyncio
async def test_sqlite_session_store_enforces_canonical_aggregate_diff(
    tmp_path: Path,
    revision_delta: int,
) -> None:
    _database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event(f"canonical-{revision_delta}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = replace(
        aggregate,
        state="review" if revision_delta == 0 else aggregate.state,
        state_revision=aggregate.state_revision + revision_delta,
        event_sequence=aggregate.event_sequence + 1,
    )

    with pytest.raises(ValueError, match="canonical aggregate diff"):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="malformed_transition",
            ),
            expected_revision=aggregate.state_revision,
        )


@pytest.mark.asyncio
async def test_sqlite_session_store_supersedes_plans_and_keeps_terminal_status(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")

    async def commit_plan(plan_id: str, plan_revision: int, event_id: str) -> None:
        await store.enqueue(_event(event_id, key))
        claim = await store.claim_next(key, worker_id="worker")
        assert claim is not None
        current = await store.load(key)
        target = current.advance(
            current_plan_id=plan_id,
            review_plan_revision=plan_revision,
            review_plan={
                "plan_id": plan_id,
                "plan_revision": plan_revision,
                "applied_delay_seconds": 30.0,
            },
        )
        schedule = SessionReviewSchedule(
            plan_id=plan_id,
            plan_revision=plan_revision,
            applied_delay_seconds=30.0,
        )
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="review_planned",
                caused_plan_id=plan_id,
                review_schedules=(schedule,),
                review_schedule_events=(
                    _scheduled_journal(
                        schedule,
                        schedule_event_id=f"{event_id}:scheduled",
                        previous_plan_id=current.current_plan_id,
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )

    await commit_plan("plan-1", 1, "plan-one")
    await commit_plan("plan-2", 2, "plan-two")
    current = await store.load(key)
    assert current.current_plan_id == "plan-2"
    assert current.review_plan_revision == 2

    await store.enqueue(_event("plan-completed", key))
    complete_claim = await store.claim_next(key, worker_id="worker")
    assert complete_claim is not None
    await store.commit(
        complete_claim,
        SessionTransition(
            aggregate=current.advance(state_changed=False),
            disposition="review_completed",
            caused_plan_id="plan-2",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-2",
                    plan_revision=2,
                    applied_delay_seconds=30.0,
                    status=ReviewScheduleStatus.COMPLETED,
                ),
            ),
        ),
        expected_revision=current.state_revision,
    )

    terminal = await store.load(key)
    await store.enqueue(_event("plan-regression", key))
    regression_claim = await store.claim_next(key, worker_id="worker")
    assert regression_claim is not None
    with pytest.raises(DurableRecordConflict, match="terminal state"):
        await store.commit(
            regression_claim,
            SessionTransition(
                aggregate=terminal.advance(state_changed=False),
                disposition="review_rescheduled",
                caused_plan_id="plan-2",
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id="plan-2",
                        plan_revision=2,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
            expected_revision=terminal.state_revision,
        )

    with database.connect() as conn:
        schedules = conn.execute(
            """
            SELECT plan_id, plan_revision, status
            FROM agent_review_schedules
            ORDER BY plan_revision
            """
        ).fetchall()
        superseded = conn.execute(
            """
            SELECT plan_id, event_type, metadata_json
            FROM agent_review_schedule_events
            WHERE event_type = 'superseded'
            """
        ).fetchone()
    assert [tuple(row) for row in schedules] == [
        ("plan-1", 1, "superseded"),
        ("plan-2", 2, "completed"),
    ]
    assert tuple(superseded) == (
        "plan-1",
        "superseded",
        '{"superseded_by_plan_id":"plan-2"}',
    )


@pytest.mark.asyncio
async def test_sqlite_session_store_preserves_operation_provenance_on_completion(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("operation-created", key))
    created_claim = await store.claim_next(key, worker_id="worker")
    assert created_claim is not None
    aggregate = await store.load(key)
    await store.commit(
        created_claim,
        SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="operation_created",
            caused_operation_id="operation-audit",
            operations=(
                SessionOperation(
                    operation_id="operation-audit",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.RUNNING,
                    metadata={"deadline_at": 130.0, "planning_input": "tail-v1"},
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )

    await store.enqueue(_event("operation-completed", key))
    completed_claim = await store.claim_next(key, worker_id="worker")
    assert completed_claim is not None
    current = await store.load(key)
    await store.commit(
        completed_claim,
        SessionTransition(
            aggregate=current.advance(state_changed=False),
            disposition="operation_completed",
            caused_operation_id="operation-audit",
            operations=(
                SessionOperation(
                    operation_id="operation-audit",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.COMPLETED,
                    metadata={"result": "planned"},
                ),
            ),
        ),
        expected_revision=current.state_revision,
    )

    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, launched_by_event_id, metadata_json
            FROM agent_session_operations
            WHERE operation_id = 'operation-audit'
            """
        ).fetchone()
    assert tuple(operation) == (
        "completed",
        "operation-created",
        '{"deadline_at":130.0,"planning_input":"tail-v1","result":"planned"}',
    )

    await store.enqueue(_event("operation-tampered", key))
    tampered_claim = await store.claim_next(key, worker_id="worker")
    assert tampered_claim is not None
    terminal = await store.load(key)
    with pytest.raises(DurableRecordConflict, match="immutable fences"):
        await store.commit(
            tampered_claim,
            SessionTransition(
                aggregate=terminal.advance(state_changed=False),
                disposition="operation_tampered",
                caused_operation_id="operation-audit",
                operations=(
                    SessionOperation(
                        operation_id="operation-audit",
                        kind="idle_review_planning",
                        status=SessionOperationStatus.COMPLETED,
                        active_epoch=99,
                    ),
                ),
            ),
            expected_revision=terminal.state_revision,
        )
