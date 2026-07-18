"""End-to-end durable coverage for the first Actor v2 review workflow slice."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runtime.review_stores import DatabaseReviewMessageStore
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
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
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    SQLiteReviewExecutionGateStore,
)
from shinbot.agent.runtime.session_actor.review_workflow import RunnerReviewWorkflow
from shinbot.agent.runtime.session_actor.review_workflow_context import (
    ActorReviewWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowOutput,
    ActiveReplyWorkflowRequest,
    register_actor_workflow_effect_handlers,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord
from tests.agent_runtime_helpers import wait_for_session_actor_idle


@dataclass(slots=True)
class _ReplyRunner:
    """Deterministic no-reply runner that records Actor-projected input."""

    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Return one pure no-reply decision with model audit provenance."""

        self.inputs.append(stage_input)
        return ReplyDecisionStageOutput(
            replied=False,
            target_message_ids=[
                message["id"]
                for message in stage_input.source_messages
                if isinstance(message.get("id"), int)
            ],
            reason="no_reply_tool",
            model_execution_id="actor-review-execution-a",
            prompt_signature="actor-review-prompt-a",
        )


class _NoopActiveReplyWorkflow:
    """Unused sibling workflow required by shared handler registration."""

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Return no work because this integration only drives review effects."""

        del request
        return ActiveReplyWorkflowOutput()


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
) -> SessionEventEnvelope:
    """Build one route-owned message delivery for the actor registry."""

    return SessionEventEnvelope(
        event_id="message-review-a",
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        source="agent_route_outbox",
        occurred_at=100.0,
        payload={
            "version": 1,
            "event_id": "message-review-a",
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
            "trace_id": "trace:message-review-a",
            "observed_at": 100.0,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        causation_id="route:message-review-a",
        correlation_id="correlation:message-review-a",
        trace_id="trace:message-review-a",
        available_at=100.0,
        created_at=100.0,
    )


def _review_due_event(
    *,
    key: SessionKey,
    generation: int,
    plan_id: str,
    plan_revision: int,
) -> SessionEventEnvelope:
    """Build a canonical durable due-review delivery for the current plan."""

    event_id = review_due_event_id(
        key=key,
        plan_id=plan_id,
        plan_revision=plan_revision,
        ownership_generation=generation,
        delivery_cycle=0,
    )
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.REVIEW_DUE,
        ownership_generation=generation,
        source=REVIEW_DUE_EVENT_SOURCE,
        occurred_at=160.0,
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "ownership_generation": generation,
            "attempt_count": 0,
        },
        trace_id="trace:review-due-a",
        available_at=160.0,
        created_at=160.0,
    )


@pytest.mark.asyncio
async def test_review_due_effect_executor_and_completion_use_one_actor_snapshot(
    tmp_path: Path,
) -> None:
    """ReviewDue reaches ReviewCompleted through the registry and durable outbox."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot-a:instance-a:base-session")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="review workflow runtime integration",
    ).ownership
    authority = builtin_effect_contract_authority()
    actor_store = SQLiteSessionActorStore(
        database,
        clock=lambda: now[0],
        effect_contract_authority=authority,
    )
    await actor_store.ensure(key, ownership_generation=ownership.generation)
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(
            default_review_delay_seconds=45.0,
            default_review_reason="review runtime integration default",
        )
    )
    registry = AgentSessionActorRegistry(store=actor_store, handler=reducer.reduce)
    reply_runner = _ReplyRunner()
    review_workflow = RunnerReviewWorkflow(
        projector=ActorReviewWorkflowContextProjector(
            message_store=DatabaseReviewMessageStore(database),
        ),
        reply_runner=reply_runner,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    register_actor_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_reply_workflow=_NoopActiveReplyWorkflow(),
        review_workflow=review_workflow,
    )
    executor = DurableEffectExecutor(
        store=SQLiteDurableEffectStore(
            database,
            clock=lambda: now[0],
            contract_authority=authority,
        ),
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=10.0,
        review_execution_gate_store=SQLiteReviewExecutionGateStore(
            database,
            clock=lambda: now[0],
        ),
        clock=lambda: now[0],
    )

    try:
        message_log_id = database.message_logs.insert(
            MessageLogRecord(
                session_id="instance-a:base-session",
                platform_msg_id="platform-review-a",
                sender_id="user-a",
                sender_name="User A",
                raw_text="review this exact durable message",
                content_json="[]",
                role="user",
                created_at=100.0,
            )
        )
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=message_log_id,
            )
        )
        await wait_for_session_actor_idle(database, registry, key)
        planned = await actor_store.load(key)

        now[0] = 160.0
        await registry.submit(
            _review_due_event(
                key=key,
                generation=ownership.generation,
                plan_id=planned.current_plan_id,
                plan_revision=planned.review_plan_revision,
            )
        )
        await wait_for_session_actor_idle(database, registry, key)
        reviewing = await actor_store.load(key)
        assert reviewing.state == AgentSessionState.REVIEW
        operation_id = reviewing.review_operation_id
        fence = reviewing.data["operation_fences"][operation_id]
        assert fence["input_watermark"] == message_log_id
        assert fence["input_ledger_sequence"] == 1

        now[0] = 200.0
        result = await executor.run_once(lane=EffectLane.PLANNER)
        assert result.status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(database, registry, key)

        settled = await actor_store.load(key)
        assert settled.state == AgentSessionState.IDLE
        assert settled.review_operation_id == ""
        assert settled.current_plan_id != planned.current_plan_id
        assert settled.review_plan["kind"] == "defaulted"
        assert settled.review_plan["applied_delay_seconds"] == 45.0
        assert settled.review_plan["model_execution_id"] == "actor-review-execution-a"
        assert settled.review_plan["prompt_signature"] == "actor-review-prompt-a"
        assert len(reply_runner.inputs) == 1
        stage_input = reply_runner.inputs[0]
        assert stage_input.session_id == key.session_id
        assert stage_input.instance_id == "instance-a"
        assert [message["id"] for message in stage_input.source_messages] == [
            message_log_id
        ]
        assert stage_input.metadata["ledger_message_log_ids"] == [message_log_id]
        assert stage_input.metadata["input_ledger_sequence"] == 1
        assert await actor_store.list_unread_messages(key) == ()

        with database.connect() as conn:
            operation = conn.execute(
                """
                SELECT status, input_watermark, input_ledger_sequence
                FROM agent_session_operations
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
            effect = conn.execute(
                """
                SELECT status FROM agent_effect_outbox
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
            completion = conn.execute(
                """
                SELECT status
                FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ? AND kind = 'ReviewCompleted'
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
        assert operation is not None
        assert tuple(operation) == ("completed", message_log_id, 1)
        assert effect is not None
        assert effect["status"] == "completed"
        assert completion is not None
        assert completion["status"] == "completed"
    finally:
        await executor.shutdown(drain=False)
        await registry.shutdown(drain=False)
