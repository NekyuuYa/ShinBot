"""End-to-end durable coverage for the Actor v2 idle-review planner slice."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from shinbot.agent.runners.review_models import IdleReviewPlanningStageOutput
from shinbot.agent.runtime.review_stores import DatabaseReviewMessageStore
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectHandlerRegistry,
    EffectLane,
    EffectRunStatus,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    RunnerIdleReviewPlanningWorkflow,
    register_idle_review_planning_effect_handler,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_context import (
    ActorIdleReviewPlanningContextProjector,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


@dataclass(slots=True)
class _Runner:
    """Deterministic model runner used to prove the effect boundary."""

    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> IdleReviewPlanningStageOutput:
        """Record the actor-projected prompt input and return one decision."""

        self.inputs.append(stage_input)
        return IdleReviewPlanningStageOutput(
            next_review_after_seconds=42.0,
            reason="durable_conversation_settled",
            model_execution_id="planner-execution-a",
            prompt_signature="planner-prompt-a",
        )


@dataclass(slots=True)
class _WakeTarget:
    """Wake target that records only post-commit mailbox notifications."""

    keys: list[SessionKey] = field(default_factory=list)

    async def wake(self, key: SessionKey) -> None:
        """Record the key whose completion mailbox event has committed."""

        self.keys.append(key)

    async def recover(self) -> int:
        """Return no additional keys for this single-effect integration test."""

        return 0


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
) -> SessionEventEnvelope:
    """Build one canonical route-to-actor message delivery."""

    return SessionEventEnvelope(
        event_id="message-a",
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        source="agent_route_outbox",
        occurred_at=100.0,
        payload={
            "version": 1,
            "event_id": "message-a",
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:base-session",
            "bot_session_id": key.session_id,
            "message_log_id": message_log_id,
            "sender_id": "user-a",
            "instance_id": "instance-a",
            "platform": "test",
            "self_id": "bot-a",
            "is_private": False,
            "is_mentioned": False,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": False,
            "is_stopped": False,
            "trace_id": "trace-a",
            "observed_at": 100.0,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        causation_id="route-a",
        correlation_id="correlation-a",
        trace_id="trace-a",
        created_at=100.0,
    )


async def _seed_active_chat(
    *,
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    reducer: AgentSessionReducer,
    key: SessionKey,
    generation: int,
    message_log_id: int,
) -> None:
    """Create one actor-owned message then a test-only active-chat state."""

    message_event = _message_event(
        key=key,
        generation=generation,
        message_log_id=message_log_id,
    )
    await store.enqueue(message_event)
    message_claim = await store.claim_next(key, worker_id="setup-worker")
    assert message_claim is not None
    initial = await store.load(key)
    after_message = await store.commit(
        message_claim,
        reducer.reduce(initial, message_event),
        expected_revision=initial.state_revision,
    )

    bootstrap = SessionEventEnvelope(
        event_id="bootstrap-active-chat",
        key=key,
        kind="TestBootstrapActiveChat",
        ownership_generation=generation,
        source="integration-test",
        occurred_at=101.0,
    )
    await store.enqueue(bootstrap)
    bootstrap_claim = await store.claim_next(key, worker_id="setup-worker")
    assert bootstrap_claim is not None
    await store.commit(
        bootstrap_claim,
        SessionTransition(
            aggregate=after_message.advance(
                state=AgentSessionState.ACTIVE_CHAT.value,
                active_epoch=after_message.active_epoch + 1,
                active_chat_state={
                    "interest_value": 4.0,
                    "entered_at": 101.0,
                    "last_message_at": 100.0,
                    "tick_count": 1,
                    "bootstrap_disposition": "continue",
                },
                updated_at=101.0,
            ),
            disposition="test_active_chat_started",
            reason="test_setup",
        ),
        expected_revision=after_message.state_revision,
    )


@pytest.mark.asyncio
async def test_durable_planner_effect_projects_ledger_and_commits_fenced_schedule(
    tmp_path: Path,
) -> None:
    """One model decision reaches IDLE only through actor/effect completion."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="idle planner runtime integration",
    ).ownership
    authority = builtin_effect_contract_authority()
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: now[0],
        effect_contract_authority=authority,
    )
    await store.ensure(key, ownership_generation=ownership.generation)
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="instance-a:base-session",
            platform_msg_id="platform-a",
            sender_id="user-a",
            sender_name="User A",
            raw_text="hello durable planner",
            content_json="[]",
            role="user",
            created_at=100.0,
        )
    )
    reducer = AgentSessionReducer()
    await _seed_active_chat(
        database=database,
        store=store,
        reducer=reducer,
        key=key,
        generation=ownership.generation,
        message_log_id=message_log_id,
    )

    runner = _Runner()
    workflow = RunnerIdleReviewPlanningWorkflow(
        projector=ActorIdleReviewPlanningContextProjector(
            ledger=store,
            message_store=DatabaseReviewMessageStore(database),
        ),
        runner=runner,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    register_idle_review_planning_effect_handler(handlers, workflow=workflow)
    wake_target = _WakeTarget()
    executor = DurableEffectExecutor(
        store=SQLiteDurableEffectStore(
            database,
            clock=lambda: now[0],
            contract_authority=authority,
        ),
        handlers=handlers,
        session_registry=wake_target,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    now[0] = 110.0
    exit_event = SessionEventEnvelope(
        event_id="exit-a",
        key=key,
        kind=AgentSessionEventKind.EXIT_REQUESTED,
        ownership_generation=ownership.generation,
        source="integration-test",
        occurred_at=110.0,
        payload={
            "operation_id": "planner-operation-a",
            "plan_id": "planner-plan-a",
            "trigger": "active_chat_decay",
            "planning_input": {"legacy": "must-not-survive"},
        },
        causation_id="active-chat-tick-a",
        correlation_id="planner-operation-a",
        trace_id="trace-a",
        created_at=110.0,
    )
    await store.enqueue(exit_event)
    exit_claim = await store.claim_next(key, worker_id="actor-worker")
    assert exit_claim is not None
    active = await store.load(key)
    await store.commit(
        exit_claim,
        reducer.reduce(active, exit_event),
        expected_revision=active.state_revision,
    )

    now[0] = 120.0
    result = await executor.run_once(lane=EffectLane.PLANNER)
    assert result.status is EffectRunStatus.COMPLETED
    assert wake_target.keys == [key]

    completion_claim = await store.claim_next(key, worker_id="actor-worker")
    assert completion_claim is not None
    assert completion_claim.envelope.kind == "IdleReviewPlanningCompleted"
    settling = await store.load(key)
    settled = await store.commit(
        completion_claim,
        reducer.reduce(settling, completion_claim.envelope),
        expected_revision=settling.state_revision,
    )

    assert settled.state == AgentSessionState.IDLE
    assert settled.current_plan_id == "planner-plan-a"
    assert settled.review_plan["requested_delay_seconds"] == 42.0
    assert settled.review_plan["applied_delay_seconds"] == 42.0
    assert settled.review_plan["reason"] == "durable_conversation_settled"
    assert settled.review_plan["model_execution_id"] == "planner-execution-a"
    assert settled.review_plan["prompt_signature"] == "planner-prompt-a"
    assert len(runner.inputs) == 1
    stage_input = runner.inputs[0]
    assert stage_input.session_id == key.session_id
    assert [message["id"] for message in stage_input.source_messages] == [
        message_log_id
    ]
    metadata = stage_input.metadata
    assert metadata["actor_v2"] is True
    assert metadata["ledger_message_log_ids"] == [message_log_id]
    assert metadata["input_ledger_sequence"] == 1
    assert "legacy" not in metadata["planning_input"]
