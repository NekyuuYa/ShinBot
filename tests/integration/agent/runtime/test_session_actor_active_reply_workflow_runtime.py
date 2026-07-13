"""End-to-end durable coverage for the Actor v2 active-reply workflow slice."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runtime.review_stores import DatabaseReviewMessageStore
from shinbot.agent.runtime.session_actor.active_reply_workflow import (
    RunnerActiveReplyWorkflow,
)
from shinbot.agent.runtime.session_actor.active_reply_workflow_context import (
    ActorActiveReplyWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.adapter_action_dispatch import (
    AdapterExternalActionDispatcher,
)
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
from shinbot.agent.runtime.session_actor.external_action_handler import (
    register_external_action_effect_handlers,
)
from shinbot.agent.runtime.session_actor.external_action_store import (
    SQLiteExternalActionReceiptStore,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionReceiptStatus,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ReviewWorkflowOutput,
    ReviewWorkflowRequest,
    register_actor_workflow_effect_handlers,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import MessageElement


@dataclass(slots=True)
class _ReplyRunner:
    """Deterministic reply runner that returns one receipt-fenced intent."""

    inputs: list[ReviewStageInput] = field(default_factory=list)

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Return a bound reply intent for the exact actor-projected message."""

        self.inputs.append(stage_input)
        message_log_ids = [
            message["id"]
            for message in stage_input.source_messages
            if isinstance(message.get("id"), int)
        ]
        assert len(message_log_ids) == 1
        message_log_id = message_log_ids[0]
        return ReplyDecisionStageOutput(
            replied=True,
            target_message_ids=[message_log_id],
            external_action_intents=(
                ExternalActionIntent(
                    kind=ExternalActionKind.SEND_REPLY,
                    tool_call_id="actor-active-reply-tool-a",
                    action_ordinal=0,
                    payload={
                        "text": "Actor v2 durable reply",
                        "quote_message_log_id": message_log_id,
                    },
                ),
            ),
            reason="send_reply_tool",
            model_execution_id="actor-active-reply-execution-a",
            prompt_signature="actor-active-reply-prompt-a",
        )


class _NoopReviewWorkflow:
    """Unused sibling workflow required by the shared handler registration."""

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Fail if this active-reply-only integration unexpectedly invokes review."""

        del request
        raise AssertionError("review workflow must not run in active-reply coverage")


class _Adapter(BaseAdapter):
    """Recording adapter with no transport side effects."""

    def __init__(self) -> None:
        super().__init__("instance-a", "test")
        self.sent: list[tuple[str, list[MessageElement]]] = []

    async def start(self) -> None:
        """Satisfy the adapter contract."""

    async def shutdown(self) -> None:
        """Satisfy the adapter contract."""

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        """Record one visible reply and return a deterministic platform handle."""

        self.sent.append((target_session, elements))
        return MessageHandle("platform-active-reply-a", adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Reject non-message platform work in this narrow reply test."""

        raise AssertionError(f"unexpected adapter API call: {method} {params}")

    async def get_capabilities(self) -> dict[str, Any]:
        """Satisfy the adapter contract."""

        return {}


@dataclass(slots=True)
class _Adapters:
    """Minimal connected adapter lookup for the real external dispatcher."""

    adapter: BaseAdapter

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        """Return the fake adapter only for its durable instance id."""

        return self.adapter if instance_id == self.adapter.instance_id else None

    def is_connected(self, instance_id: str) -> bool:
        """Report the one fake adapter as connected."""

        return instance_id == self.adapter.instance_id


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
) -> SessionEventEnvelope:
    """Build one high-priority route-owned message delivery for the actor."""

    return SessionEventEnvelope(
        event_id="message-active-reply-a",
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        source="agent_route_outbox",
        occurred_at=100.0,
        payload={
            "version": 1,
            "event_id": "message-active-reply-a",
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
            "is_mentioned": True,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": False,
            "is_stopped": False,
            "trace_id": "trace:message-active-reply-a",
            "observed_at": 100.0,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        causation_id="route:message-active-reply-a",
        correlation_id="correlation:message-active-reply-a",
        trace_id="trace:message-active-reply-a",
        available_at=100.0,
        created_at=100.0,
    )


@pytest.mark.asyncio
async def test_active_reply_workflow_materializes_one_receipt_fenced_reply(
    tmp_path: Path,
) -> None:
    """A mentioned message reaches one completed adapter reply through SQLite."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot-a:instance-a:base-session")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="active reply workflow runtime integration",
    ).ownership
    authority = builtin_effect_contract_authority()
    actor_store = SQLiteSessionActorStore(
        database,
        clock=lambda: now[0],
        effect_contract_authority=authority,
    )
    await actor_store.ensure(key, ownership_generation=ownership.generation)
    registry = AgentSessionActorRegistry(
        store=actor_store,
        handler=AgentSessionReducer(
            config=IdleExitReducerConfig(
                default_review_delay_seconds=45.0,
                default_review_reason="active reply runtime integration default",
            )
        ).reduce,
    )
    reply_runner = _ReplyRunner()
    active_reply_workflow = RunnerActiveReplyWorkflow(
        projector=ActorActiveReplyWorkflowContextProjector(
            message_store=DatabaseReviewMessageStore(database),
        ),
        reply_runner=reply_runner,
    )
    adapter = _Adapter()
    handlers = EffectHandlerRegistry(contract_authority=authority)
    register_actor_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_reply_workflow=active_reply_workflow,
        review_workflow=_NoopReviewWorkflow(),
    )
    receipt_store = SQLiteExternalActionReceiptStore(
        database,
        clock=lambda: now[0],
    )
    register_external_action_effect_handlers(
        handlers,
        receipts=receipt_store,
        dispatcher=AdapterExternalActionDispatcher(
            adapters=_Adapters(adapter),
            database=database,
            clock=lambda: now[0],
        ),
    )
    executor = DurableEffectExecutor(
        store=SQLiteDurableEffectStore(
            database,
            clock=lambda: now[0],
            contract_authority=authority,
        ),
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    try:
        message_log_id = database.message_logs.insert(
            MessageLogRecord(
                session_id="instance-a:base-session",
                platform_msg_id="platform-active-reply-request-a",
                sender_id="user-a",
                sender_name="User A",
                raw_text="please reply through the durable actor path",
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
        await registry.wait_idle(key)

        active = await actor_store.load(key)
        assert active.state == AgentSessionState.ACTIVE_REPLY
        operation_id = active.active_reply_operation_id
        assert operation_id
        assert active.data["operation_fences"][operation_id]["message_log_ids"] == [
            message_log_id
        ]

        now[0] = 150.0
        planner_result = await executor.run_once(lane=EffectLane.PLANNER)
        assert planner_result.status is EffectRunStatus.COMPLETED
        await registry.wait_idle(key)

        after_planner = await actor_store.load(key)
        assert after_planner.state == AgentSessionState.IDLE
        assert after_planner.active_reply_operation_id == ""
        assert await actor_store.list_unread_messages(key) == ()
        assert len(reply_runner.inputs) == 1
        stage_input = reply_runner.inputs[0]
        assert stage_input.session_id == key.session_id
        assert stage_input.instance_id == "instance-a"
        assert [message["id"] for message in stage_input.source_messages] == [
            message_log_id
        ]
        assert stage_input.metadata["ledger_message_log_ids"] == [message_log_id]

        with database.connect() as conn:
            operation = conn.execute(
                """
                SELECT status, metadata_json
                FROM agent_session_operations
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
            workflow_effect = conn.execute(
                """
                SELECT status
                FROM agent_effect_outbox
                WHERE operation_id = ? AND kind = 'run_active_reply_workflow'
                """,
                (operation_id,),
            ).fetchone()
            action_effect = conn.execute(
                """
                SELECT effect_id, idempotency_key, status
                FROM agent_effect_outbox
                WHERE operation_id = ? AND kind = 'send_reply'
                """,
                (operation_id,),
            ).fetchone()
        assert operation is not None
        assert operation["status"] == "completed"
        operation_metadata = json.loads(operation["metadata_json"])
        assert operation_metadata["model_execution_id"] == "actor-active-reply-execution-a"
        assert operation_metadata["prompt_signature"] == "actor-active-reply-prompt-a"
        assert workflow_effect is not None
        assert workflow_effect["status"] == "completed"
        assert action_effect is not None
        assert action_effect["status"] == "pending"
        assert set(after_planner.data["pending_outbound_actions"]) == {
            action_effect["effect_id"]
        }

        now[0] = 200.0
        action_result = await executor.run_once(lane=EffectLane.DEFAULT)
        assert action_result.status is EffectRunStatus.COMPLETED
        await registry.wait_idle(key)

        settled = await actor_store.load(key)
        assert settled.state == AgentSessionState.IDLE
        assert "pending_outbound_actions" not in settled.data
        assert len(adapter.sent) == 1
        target_session, elements = adapter.sent[0]
        assert target_session == "instance-a:base-session"
        assert [(element.type, element.attrs) for element in elements] == [
            ("quote", {"id": "platform-active-reply-request-a"}),
            ("text", {"content": "Actor v2 durable reply"}),
        ]

        receipt = await receipt_store.get(key, action_effect["idempotency_key"])
        assert receipt is not None
        assert receipt.status is ExternalActionReceiptStatus.SUCCEEDED
        assert receipt.assistant_message_log_id is not None
        assistant_message = database.message_logs.get(receipt.assistant_message_log_id)
        assert assistant_message is not None
        assert assistant_message["role"] == "assistant"
        assert assistant_message["session_id"] == "instance-a:base-session"
        assert assistant_message["platform_msg_id"] == "platform-active-reply-a"
        assert assistant_message["raw_text"] == "Actor v2 durable reply"

        with database.connect() as conn:
            action_effect_status = conn.execute(
                """
                SELECT status
                FROM agent_effect_outbox
                WHERE effect_id = ?
                """,
                (action_effect["effect_id"],),
            ).fetchone()
            completion = conn.execute(
                """
                SELECT status
                FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ?
                  AND kind = 'ExternalActionCompleted'
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
        assert action_effect_status is not None
        assert action_effect_status["status"] == "completed"
        assert completion is not None
        assert completion["status"] == "completed"
    finally:
        await executor.shutdown(drain=False)
        await registry.shutdown(drain=False)
