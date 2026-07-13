"""SQLite integration coverage for actor-owned active-chat round fencing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
    EffectRunStatus,
)
from shinbot.agent.runtime.session_actor.effect_store import (
    SQLiteDurableEffectStore,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionKind,
    builtin_external_action_effect_contract,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEffectKind,
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
    is_mentioned: bool = False,
) -> SessionEventEnvelope:
    event_id = f"message:{message_log_id}"
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        source="agent_route_outbox",
        occurred_at=10.0,
        causation_id=f"route:{event_id}",
        trace_id=f"trace:{event_id}",
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:base-a",
            "bot_session_id": key.session_id,
            "message_log_id": message_log_id,
            "sender_id": "user-a",
            "instance_id": "instance-a",
            "platform": "test",
            "self_id": "bot-a",
            "is_private": False,
            "is_mentioned": is_mentioned,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": False,
            "is_stopped": False,
            "trace_id": f"trace:{event_id}",
            "observed_at": 10.0,
            "event_type": "message-created",
        },
    )


async def _seed_active_chat(
    store: SQLiteSessionActorStore,
    *,
    key: SessionKey,
    generation: int,
    interest_value: float = 20.0,
    message_watermark: int = 0,
) -> None:
    """Commit an active-chat aggregate before exercising durable effects."""

    await store.ensure(key, ownership_generation=generation)
    bootstrap = SessionEventEnvelope(
        event_id="bootstrap-active-chat",
        key=key,
        kind="TestBootstrapActiveChat",
        ownership_generation=generation,
        occurred_at=1.0,
    )
    await store.enqueue(bootstrap)
    claim = await store.claim_next(key, worker_id="bootstrap-worker")
    assert claim is not None
    initial = await store.load(key)
    await store.commit(
        claim,
        SessionTransition(
            aggregate=initial.advance(
                state=AgentSessionState.ACTIVE_CHAT.value,
                active_epoch=1,
                current_plan_id="plan-a",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "plan-a",
                    "plan_revision": 1,
                    "applied_delay_seconds": 900.0,
                    "trigger": "test_active_chat_bootstrap",
                    "kind": "defaulted",
                    "source": "integration-test",
                },
                active_chat_state={
                    "active_epoch": 1,
                    "interest_value": interest_value,
                    "decay_half_life_seconds": 20.0,
                    "entered_at": 1.0,
                    "updated_at": 1.0,
                    "tick_count": 0,
                    "pending_message_log_ids": [],
                    "bootstrap_status": "completed",
                    "bootstrap_operation_id": "",
                    "round_schedule_revision": 0,
                    "round_schedule_id": "",
                    "round_due_at": None,
                },
                data={
                    "message_watermark": message_watermark,
                    "delivery_context": {
                        "instance_id": "instance-a",
                        "target_session_id": "instance-a:base-a",
                    },
                },
                updated_at=initial.updated_at,
            ),
            disposition="active_chat_bootstrapped",
            caused_plan_id="plan-a",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-a",
                    plan_revision=1,
                    applied_delay_seconds=900.0,
                    trigger="test_active_chat_bootstrap",
                    outcome="defaulted",
                    source="integration-test",
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="seed-active-chat-plan-scheduled",
                    event_type="scheduled",
                    plan_id="plan-a",
                    trigger="test_active_chat_bootstrap",
                    outcome="defaulted",
                    source="integration-test",
                    applied_delay_seconds=900.0,
                    metadata={
                        "plan_revision": 1,
                        "schedule_outcome": {
                            "active_reply_threshold": {},
                            "applied_delay_seconds": 900.0,
                            "fallback_reason": "",
                            "kind": "defaulted",
                            "mention_sensitivity": "normal",
                            "model_execution_id": "",
                            "prompt_signature": "",
                            "reason": "",
                            "requested_delay_seconds": None,
                            "source": "integration-test",
                        },
                    },
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )


@pytest.mark.asyncio
async def test_active_chat_round_snapshot_commits_without_consuming_later_work(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="active chat store test",
        legacy_session_id="legacy:session-a",
    ).ownership
    await store.ensure(key, ownership_generation=ownership.generation)

    bootstrap = SessionEventEnvelope(
        event_id="bootstrap-active-chat",
        key=key,
        kind="TestBootstrapActiveChat",
        ownership_generation=ownership.generation,
        occurred_at=1.0,
    )
    await store.enqueue(bootstrap)
    claim = await store.claim_next(key, worker_id="bootstrap-worker")
    assert claim is not None
    initial = await store.load(key)
    active = await store.commit(
        claim,
        SessionTransition(
            aggregate=initial.advance(
                state=AgentSessionState.ACTIVE_CHAT.value,
                active_epoch=1,
                current_plan_id="plan-a",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "plan-a",
                    "plan_revision": 1,
                    "applied_delay_seconds": 900.0,
                    "trigger": "test_active_chat_bootstrap",
                    "kind": "defaulted",
                    "source": "integration-test",
                },
                active_chat_state={
                    "active_epoch": 1,
                    "interest_value": 20.0,
                    "decay_half_life_seconds": 20.0,
                    "entered_at": 1.0,
                    "updated_at": 1.0,
                    "tick_count": 0,
                    "pending_message_log_ids": [],
                    "bootstrap_status": "completed",
                    "bootstrap_operation_id": "",
                    "round_schedule_revision": 0,
                    "round_schedule_id": "",
                    "round_due_at": None,
                },
                data={
                    "message_watermark": 0,
                    "delivery_context": {
                        "instance_id": "instance-a",
                        "target_session_id": "instance-a:base-a",
                    },
                },
            ),
            disposition="active_chat_bootstrapped",
            caused_plan_id="plan-a",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-a",
                    plan_revision=1,
                    applied_delay_seconds=900.0,
                    trigger="test_active_chat_bootstrap",
                    outcome="defaulted",
                    source="integration-test",
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="active-chat-plan-scheduled",
                    event_type="scheduled",
                    plan_id="plan-a",
                    trigger="test_active_chat_bootstrap",
                    outcome="defaulted",
                    source="integration-test",
                    applied_delay_seconds=900.0,
                    metadata={
                        "plan_revision": 1,
                        "schedule_outcome": {
                            "active_reply_threshold": {},
                            "applied_delay_seconds": 900.0,
                            "fallback_reason": "",
                            "kind": "defaulted",
                            "mention_sensitivity": "normal",
                            "model_execution_id": "",
                            "prompt_signature": "",
                            "reason": "",
                            "requested_delay_seconds": None,
                            "source": "integration-test",
                        },
                    },
                ),
            ),
        ),
        expected_revision=initial.state_revision,
    )
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="base-a",
            platform_msg_id="platform:active-1",
            sender_id="user-a",
            sender_name="User A",
            raw_text="hello",
            content_json="[]",
            role="user",
            created_at=10.0,
        )
    )
    message = _message_event(
        key=key,
        generation=ownership.generation,
        message_log_id=message_log_id,
    )
    await store.enqueue(message)
    message_claim = await store.claim_next(key, worker_id="message-worker")
    assert message_claim is not None
    buffered = await store.commit(
        message_claim,
        reducer.reduce(active, message),
        expected_revision=active.state_revision,
    )
    state = buffered.active_chat_state
    round_intent = buffered.data["effect_control_intents"][
        "enqueue_active_chat_round_due"
    ]
    due_contract = builtin_effect_contract("enqueue_active_chat_round_due")
    due = SessionEventEnvelope(
        event_id=str(state["round_due_event_id"]),
        key=key,
        kind=AgentSessionEventKind.ACTIVE_CHAT_ROUND_DUE,
        ownership_generation=ownership.generation,
        source=due_contract.completion_source,
        occurred_at=20.0,
        causation_id=str(state["round_schedule_source_event_id"]),
        payload={
            "effect_id": state["round_schedule_effect_id"],
                "effect_kind": due_contract.effect_kind,
                "idempotency_key": state["round_schedule_effect_id"],
                "operation_id": "",
                "plan_id": round_intent["plan_id"],
                "schedule_id": state["round_schedule_id"],
                "schedule_revision": state["round_schedule_revision"],
                "active_epoch": 1,
                "activity_generation": round_intent["activity_generation"],
                "input_watermark": state["round_schedule_input_watermark"],
                "input_ledger_sequence": None,
                "attempt_count": 1,
            "contract_version": due_contract.version,
            "contract_signature": due_contract.signature,
        },
    )
    await store.enqueue(due)
    due_claim = await store.claim_next(key, worker_id="round-worker")
    assert due_claim is not None
    running = await store.commit(
        due_claim,
        reducer.reduce(buffered, due),
        expected_revision=buffered.state_revision,
    )
    operation_id = running.active_chat_round_operation_id
    fence = running.data["operation_fences"][operation_id]
    assert fence["input_ledger_sequence"] == 1
    round_contract = builtin_effect_contract("run_active_chat_round")
    completion = SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=key,
        kind=AgentSessionEventKind.ACTIVE_CHAT_ROUND_COMPLETED,
        ownership_generation=ownership.generation,
        source=round_contract.completion_source,
        occurred_at=30.0,
        causation_id=str(fence["source_event_id"]),
        payload={
            "effect_id": fence["effect_id"],
            "effect_kind": round_contract.effect_kind,
            "idempotency_key": fence["idempotency_key"],
            "operation_id": operation_id,
            "plan_id": fence["plan_id"],
            "active_epoch": fence["active_epoch"],
            "activity_generation": fence["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": fence["input_ledger_sequence"],
            "attempt_count": 1,
            "contract_version": round_contract.version,
            "contract_signature": round_contract.signature,
            "workflow_result": {
                "schema_version": 1,
                "completion_type": "active_chat_round",
                "consumed_message_log_ids": [message_log_id],
                "external_actions": {"schema_version": 1, "intents": []},
                "outcome": "continue",
                "interest_delta": 1.0,
                "reason": "observed message",
            },
        },
    )
    await store.enqueue(completion)
    completion_claim = await store.claim_next(key, worker_id="round-worker")
    assert completion_claim is not None
    committed = await store.commit(
        completion_claim,
        reducer.reduce(running, completion),
        expected_revision=running.state_revision,
    )

    assert committed.state == AgentSessionState.ACTIVE_CHAT
    assert committed.active_chat_round_operation_id == ""
    entries = await store.list_message_ledger(key)
    assert len(entries) == 1
    assert entries[0].chat_consumption is not None
    assert entries[0].chat_consumption.input_ledger_sequence == 1


@pytest.mark.asyncio
async def test_v2_round_due_completion_flows_from_executor_to_actor(
    tmp_path: Path,
) -> None:
    """A v2 round-control completion must wake the actor and start one round."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(
        database,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(active_chat_semantic_wait_seconds=0.0)
    )
    key = SessionKey("profile-a", "session-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="active chat executor completion test",
        legacy_session_id="legacy:session-a",
    ).ownership
    await _seed_active_chat(store, key=key, generation=ownership.generation)
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="base-a",
            platform_msg_id="platform:active-executor-1",
            sender_id="user-a",
            sender_name="User A",
            raw_text="hello",
            content_json="[]",
            role="user",
            created_at=10.0,
        )
    )
    registry = AgentSessionActorRegistry(
        store=store,
        handler=reducer.reduce,
        retry_delay_seconds=0.0,
    )
    round_due_contract = builtin_effect_contract(
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
    )
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)

    async def enqueue_round_due(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        return EffectHandlerResult()

    handlers.register(
        round_due_contract.effect_kind,
        enqueue_round_due,
        contract=round_due_contract,
    )
    executor = DurableEffectExecutor(
        store=SQLiteDurableEffectStore(database, clock=lambda: now[0]),
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    try:
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=message_log_id,
            )
        )
        await registry.wait_idle(key)
        scheduled = await store.load(key)
        intent = scheduled.data["effect_control_intents"][
            AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
        ]

        assert round_due_contract.version == 2
        assert intent["contract_version"] == round_due_contract.version
        assert intent["contract_signature"] == round_due_contract.signature

        result = await executor.run_once(lane=EffectLane.CONTROL)
        await registry.wait_idle(key)
    finally:
        await registry.shutdown()

    assert result.status == EffectRunStatus.COMPLETED
    assert result.event_id == intent["completion_event_id"]
    settled = await store.load(key)
    settled_intent = settled.data["effect_control_intents"][
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
    ]
    assert settled_intent["status"] == "completed"
    assert settled.active_chat_round_operation_id
    assert settled.active_chat_state["round_operation_id"] == (
        settled.active_chat_round_operation_id
    )

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, contract_version
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (intent["effect_id"],),
        ).fetchone()
        completion = conn.execute(
            """
            SELECT status, kind
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, result.event_id),
        ).fetchone()

    assert effect is not None
    assert tuple(effect) == ("completed", round_due_contract.version)
    assert completion is not None
    assert tuple(completion) == (
        "completed",
        AgentSessionEventKind.ACTIVE_CHAT_ROUND_DUE,
    )


@pytest.mark.asyncio
async def test_v2_exit_failure_flows_from_executor_to_actor_blocker(
    tmp_path: Path,
) -> None:
    """A terminal v2 exit-control failure must be reduced into a visible blocker."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(
        database,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=1)
    )
    key = SessionKey("profile-a", "session-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="active chat executor failure test",
        legacy_session_id="legacy:session-a",
    ).ownership
    await _seed_active_chat(
        store,
        key=key,
        generation=ownership.generation,
        interest_value=1.0,
    )
    registry = AgentSessionActorRegistry(
        store=store,
        handler=reducer.reduce,
        retry_delay_seconds=0.0,
    )
    exit_contract = builtin_effect_contract(
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
    )
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handler_attempts = 0

    async def fail_exit_request(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        nonlocal handler_attempts
        handler_attempts += 1
        raise RuntimeError("simulated exit request failure")

    handlers.register(
        exit_contract.effect_kind,
        fail_exit_request,
        contract=exit_contract,
    )
    executor = DurableEffectExecutor(
        store=SQLiteDurableEffectStore(database, clock=lambda: now[0]),
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    tick = SessionEventEnvelope(
        event_id="active-chat-tick:exit",
        key=key,
        kind=AgentSessionEventKind.ACTIVE_CHAT_TICK,
        ownership_generation=ownership.generation,
        source="active_chat_timer",
        occurred_at=now[0],
        payload={
            "active_epoch": 1,
            "expected_message_watermark": 0,
            "ownership_generation": ownership.generation,
        },
    )
    try:
        await registry.submit(tick)
        await registry.wait_idle(key)
        requested = await store.load(key)
        intent = requested.data["effect_control_intents"][
            AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
        ]

        assert exit_contract.version == 2
        assert intent["contract_version"] == exit_contract.version
        assert intent["contract_signature"] == exit_contract.signature

        first = await executor.run_once(lane=EffectLane.CONTROL)
        assert first.status == EffectRunStatus.RETRY_SCHEDULED
        assert first.retry_at is not None
        now[0] = first.retry_at
        second = await executor.run_once(lane=EffectLane.CONTROL)
        assert second.status == EffectRunStatus.RETRY_SCHEDULED
        assert second.retry_at is not None
        now[0] = second.retry_at
        terminal = await executor.run_once(lane=EffectLane.CONTROL)
        await registry.wait_idle(key)
    finally:
        await registry.shutdown()

    assert terminal.status == EffectRunStatus.FAILED
    assert terminal.event_id == intent["failure_event_id"]
    assert handler_attempts == exit_contract.max_attempts
    blocked = await store.load(key)
    blocked_intent = blocked.data["effect_control_intents"][
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
    ]
    assert blocked.state == AgentSessionState.ACTIVE_CHAT
    assert blocked_intent["status"] == "failed"
    assert blocked_intent["last_failure"]["failure_code"] == "RuntimeError"
    assert blocked.active_chat_state["exit_requested"] is False
    assert blocked.active_chat_state["exit_blocker"] == {
        "effect_id": intent["effect_id"],
        "failure_event_id": terminal.event_id,
        "failure_code": "RuntimeError",
    }

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, contract_version
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (intent["effect_id"],),
        ).fetchone()
        failure = conn.execute(
            """
            SELECT status, kind
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, terminal.event_id),
        ).fetchone()

    assert effect is not None
    assert tuple(effect) == ("failed", exit_contract.version)
    assert failure is not None
    assert tuple(failure) == ("completed", AgentSessionEventKind.EFFECT_FAILED)


@pytest.mark.asyncio
async def test_external_action_v2_authority_overrides_handler_identity_and_releases_pending(
    tmp_path: Path,
) -> None:
    """A forged handler projection cannot strand an accepted external action."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(
        database,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "session-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="external action authority integration test",
        legacy_session_id="legacy:session-a",
    ).ownership
    await store.ensure(key, ownership_generation=ownership.generation)
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="base-a",
            platform_msg_id="platform:external-authority-1",
            sender_id="user-a",
            sender_name="User A",
            raw_text="please reply",
            content_json="[]",
            role="user",
            created_at=10.0,
        )
    )
    registry = AgentSessionActorRegistry(
        store=store,
        handler=reducer.reduce,
        retry_delay_seconds=0.0,
    )
    try:
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=message_log_id,
                is_mentioned=True,
            )
        )
        await registry.wait_idle(key)
        replying = await store.load(key)
        assert replying.state == AgentSessionState.ACTIVE_REPLY
        operation_id = replying.active_reply_operation_id
        fence = replying.data["operation_fences"][operation_id]
        workflow_contract = builtin_effect_contract(
            AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW
        )
        workflow_completion = SessionEventEnvelope(
            event_id=str(fence["completion_event_id"]),
            key=key,
            kind=AgentSessionEventKind.ACTIVE_REPLY_COMPLETED,
            ownership_generation=ownership.generation,
            source=workflow_contract.completion_source,
            occurred_at=110.0,
            causation_id=str(fence["source_event_id"]),
            correlation_id=operation_id,
            trace_id="trace:external-authority-workflow",
            payload={
                "effect_id": fence["effect_id"],
                "effect_kind": workflow_contract.effect_kind,
                "idempotency_key": fence["idempotency_key"],
                "operation_id": operation_id,
                "plan_id": fence["plan_id"],
                "active_epoch": fence["active_epoch"],
                "activity_generation": fence["activity_generation"],
                "input_watermark": fence["input_watermark"],
                "input_ledger_sequence": fence["input_ledger_sequence"],
                "attempt_count": 1,
                "contract_version": workflow_contract.version,
                "contract_signature": workflow_contract.signature,
                "workflow_result": {
                    "schema_version": 1,
                    "completion_type": "active_reply",
                    "consumed_message_log_ids": [message_log_id],
                    "external_actions": {
                        "schema_version": 1,
                        "intents": [
                            {
                                "proposal_id": "reply-proposal-a",
                                "action_ordinal": 0,
                                "kind": ExternalActionKind.SEND_REPLY.value,
                                "payload": {"text": "acknowledged"},
                            }
                        ],
                    },
                },
            },
        )
        await registry.submit(workflow_completion)
        await registry.wait_idle(key)
        waiting = await store.load(key)
        pending = waiting.data["pending_outbound_actions"]
        assert len(pending) == 1
        effect_id, pending_action = next(iter(pending.items()))
        action_contract = builtin_external_action_effect_contract(
            ExternalActionKind.SEND_REPLY
        )
        assert pending_action["contract_version"] == 2
        assert pending_action["contract_signature"] == action_contract.signature

        calls = 0

        async def forged_handler(
            _context: EffectExecutionContext,
        ) -> EffectHandlerResult:
            nonlocal calls
            calls += 1
            return EffectHandlerResult(
                payload={
                    "action_ordinal": 999,
                    "request_digest": "0" * 64,
                    "receipt_status": "succeeded",
                }
            )

        handlers = EffectHandlerRegistry(include_builtin_contracts=False)
        handlers.register(
            action_contract.effect_kind,
            forged_handler,
            contract=action_contract,
        )
        executor = DurableEffectExecutor(
            store=SQLiteDurableEffectStore(database, clock=lambda: now[0]),
            handlers=handlers,
            session_registry=registry,
            renew_interval_seconds=None,
            clock=lambda: now[0],
        )
        result = await executor.run_once(lane=EffectLane.DEFAULT)
        await registry.wait_idle(key)
    finally:
        await registry.shutdown()

    assert result.status is EffectRunStatus.COMPLETED
    assert calls == 1
    settled = await store.load(key)
    assert "pending_outbound_actions" not in settled.data
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT kind, status, payload_json
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, result.event_id),
        ).fetchone()
        effect = conn.execute(
            """
            SELECT status, contract_version
            FROM agent_effect_outbox WHERE effect_id = ?
            """,
            (effect_id,),
        ).fetchone()
    assert mailbox is not None
    payload = json.loads(str(mailbox["payload_json"]))
    assert tuple(mailbox)[:2] == (
        AgentSessionEventKind.EXTERNAL_ACTION_COMPLETED,
        "completed",
    )
    assert payload["action_ordinal"] == pending_action["action_ordinal"]
    assert payload["request_digest"] == pending_action["request_digest"]
    assert effect is not None
    assert tuple(effect) == ("completed", 2)
