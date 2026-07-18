"""SQLite coverage for the Actor-native Active Chat v3 workflow slice."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.adapter_action_dispatch import (
    AdapterExternalActionDispatcher,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.delayed_control_handler import (
    register_delayed_control_effect_handlers,
)
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
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    SQLiteReviewExecutionGateStore,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveChatBootstrapWorkflowOutput,
    ActiveChatBootstrapWorkflowRequest,
    ActiveChatRoundWorkflowOutput,
    ActiveChatRoundWorkflowRequest,
    ActiveReplyWorkflowOutput,
    ActiveReplyWorkflowRequest,
    ReviewWorkflowOutput,
    ReviewWorkflowRequest,
    ReviewWorkflowWindowOutput,
    register_actor_active_chat_workflow_effect_handlers,
    register_actor_workflow_effect_handlers,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapDisposition,
    ActiveChatRoundOutcome,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import MessageElement
from tests.agent_runtime_helpers import wait_for_session_actor_idle


@dataclass(slots=True)
class _ReviewWorkflow:
    """Deterministic review that enters Active Chat from exactly its snapshot."""

    requests: list[ReviewWorkflowRequest] = field(default_factory=list)
    include_outbound_reply: bool = False

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Consume the frozen review input and request an Active Chat handoff."""

        self.requests.append(request)
        if self.include_outbound_reply:
            return ReviewWorkflowOutput(
                enter_active_chat=True,
                next_review_outcome=None,
                reply_windows=(
                    ReviewWorkflowWindowOutput(
                        window_id="review-reply-gate",
                        consumed_message_log_ids=request.effect.message_log_ids,
                        external_action_intents=(
                            ExternalActionIntent(
                                kind=ExternalActionKind.SEND_REPLY,
                                tool_call_id="review-handoff-reply",
                                action_ordinal=0,
                                payload={
                                    "text": "Review reply before active chat bootstrap",
                                    "quote_message_log_id": (
                                        request.effect.message_log_ids[0]
                                    ),
                                },
                            ),
                        ),
                    ),
                ),
                model_execution_id="review-execution-v3",
                prompt_signature="review-prompt-v3",
            )
        return ReviewWorkflowOutput(
            enter_active_chat=True,
            next_review_outcome=None,
            consumed_message_log_ids=request.effect.message_log_ids,
            model_execution_id="review-execution-v3",
            prompt_signature="review-prompt-v3",
        )


class _NoopActiveReplyWorkflow:
    """Unused sibling required by the shared Actor workflow registration."""

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Reject unexpected high-priority work in this ordinary-message flow."""

        del request
        raise AssertionError("active reply must not run in active chat coverage")


@dataclass(slots=True)
class _BootstrapWorkflow:
    """Bootstrap fake that exposes its immutable review handoff request."""

    requests: list[ActiveChatBootstrapWorkflowRequest] = field(default_factory=list)

    async def run_active_chat_bootstrap(
        self,
        request: ActiveChatBootstrapWorkflowRequest,
    ) -> ActiveChatBootstrapWorkflowOutput:
        """Return a discrete reducer-owned curve choice with provenance."""

        self.requests.append(request)
        return ActiveChatBootstrapWorkflowOutput(
            disposition=ActiveChatBootstrapDisposition.ENGAGED,
            reason="review handoff warrants engagement",
            model_execution_id="bootstrap-execution-v3",
            prompt_signature="bootstrap-prompt-v3",
        )


@dataclass(slots=True)
class _RoundWorkflow:
    """Round fake that returns one bound deferred reply and no direct effect."""

    requests: list[ActiveChatRoundWorkflowRequest] = field(default_factory=list)

    async def run_active_chat_round(
        self,
        request: ActiveChatRoundWorkflowRequest,
    ) -> ActiveChatRoundWorkflowOutput:
        """Consume exactly the request selection and defer one quote-bound reply."""

        self.requests.append(request)
        quote_message_log_id = request.message_log_ids[0]
        return ActiveChatRoundWorkflowOutput(
            outcome=ActiveChatRoundOutcome.CONTINUE,
            interest_delta=5.0,
            reason="reply to the new active-chat message",
            consumed_message_log_ids=request.message_log_ids,
            external_action_intents=(
                ExternalActionIntent(
                    kind=ExternalActionKind.SEND_REPLY,
                    tool_call_id="active-chat-round-reply",
                    action_ordinal=0,
                    payload={
                        "text": "Actor v3 durable active chat reply",
                        "quote_message_log_id": quote_message_log_id,
                    },
                ),
            ),
            model_execution_id="round-execution-v3",
            prompt_signature="round-prompt-v3",
        )


class _Adapter(BaseAdapter):
    """Recording platform adapter used by the real receipt dispatcher."""

    def __init__(self) -> None:
        super().__init__("instance-a", "test")
        self.sent: list[tuple[str, list[MessageElement]]] = []

    async def start(self) -> None:
        """Satisfy the platform adapter lifecycle contract."""

    async def shutdown(self) -> None:
        """Satisfy the platform adapter lifecycle contract."""

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        """Record one receipt-fenced visible reply."""

        self.sent.append((target_session, elements))
        return MessageHandle("platform-active-chat-round", adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, object]) -> object:
        """Reject non-message actions in this reply-only integration path."""

        raise AssertionError(f"unexpected adapter API call: {method} {params}")

    async def get_capabilities(self) -> dict[str, object]:
        """Satisfy the platform adapter contract."""

        return {}


@dataclass(slots=True)
class _Adapters:
    """Minimal connected adapter lookup consumed by the external dispatcher."""

    adapter: BaseAdapter

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        """Return the one test adapter for its exact durable instance id."""

        return self.adapter if instance_id == self.adapter.instance_id else None

    def is_connected(self, instance_id: str) -> bool:
        """Report the one adapter as connected."""

        return instance_id == self.adapter.instance_id


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
    event_id: str,
    occurred_at: float,
) -> SessionEventEnvelope:
    """Build one normal ingress delivery for the Actor v3 integration path."""

    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        source="agent_route_outbox",
        occurred_at=occurred_at,
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
            "trace_id": f"trace:{event_id}",
            "observed_at": occurred_at,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        causation_id=f"route:{event_id}",
        correlation_id=f"correlation:{event_id}",
        trace_id=f"trace:{event_id}",
        available_at=occurred_at,
        created_at=occurred_at,
    )


def _review_due_event(
    *,
    key: SessionKey,
    generation: int,
    plan_id: str,
    plan_revision: int,
    occurred_at: float,
) -> SessionEventEnvelope:
    """Build a canonical review-due delivery for the current durable plan."""

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
        occurred_at=occurred_at,
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
        trace_id="trace:active-chat-review-due",
        available_at=occurred_at,
        created_at=occurred_at,
    )


def _insert_message(
    database: DatabaseManager,
    *,
    platform_message_id: str,
    text: str,
    created_at: float,
) -> int:
    """Persist one platform message before its durable Actor delivery."""

    return database.message_logs.insert(
        MessageLogRecord(
            session_id="instance-a:base-session",
            platform_msg_id=platform_message_id,
            sender_id="user-a",
            sender_name="User A",
            raw_text=text,
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


@pytest.mark.asyncio
async def test_actor_v3_active_chat_runs_review_handoff_round_and_receipt(
    tmp_path: Path,
) -> None:
    """Review, bootstrap, round, and send remain one receipt-fenced sequence."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot-a:instance-a:base-session")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="active chat v3 runtime integration",
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
                active_chat_semantic_wait_seconds=0.0,
            )
        ).reduce,
    )
    review_workflow = _ReviewWorkflow()
    bootstrap_workflow = _BootstrapWorkflow()
    round_workflow = _RoundWorkflow()
    adapter = _Adapter()
    handlers = EffectHandlerRegistry(contract_authority=authority)
    register_actor_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_reply_workflow=_NoopActiveReplyWorkflow(),
        review_workflow=review_workflow,
    )
    register_actor_active_chat_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_chat_bootstrap_workflow=bootstrap_workflow,
        active_chat_round_workflow=round_workflow,
    )
    register_delayed_control_effect_handlers(handlers)
    receipt_store = SQLiteExternalActionReceiptStore(database, clock=lambda: now[0])
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
        renew_interval_seconds=10.0,
        review_execution_gate_store=SQLiteReviewExecutionGateStore(
            database,
            clock=lambda: now[0],
        ),
        clock=lambda: now[0],
    )

    try:
        review_message_log_id = _insert_message(
            database,
            platform_message_id="platform-review-handoff",
            text="start a durable active chat handoff",
            created_at=100.0,
        )
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=review_message_log_id,
                event_id="message:review-handoff",
                occurred_at=100.0,
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="initial review message",
        )
        planned = await actor_store.load(key)

        now[0] = 150.0
        await registry.submit(
            _review_due_event(
                key=key,
                generation=ownership.generation,
                plan_id=planned.current_plan_id,
                plan_revision=planned.review_plan_revision,
                occurred_at=now[0],
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="review due delivery",
        )
        reviewing = await actor_store.load(key)
        review_operation_id = reviewing.review_operation_id
        assert reviewing.state == AgentSessionState.REVIEW

        now[0] = 200.0
        assert (await executor.run_once(lane=EffectLane.PLANNER)).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="review workflow completion",
        )
        bootstrapping = await actor_store.load(key)
        bootstrap_operation_id = str(
            bootstrapping.active_chat_state["bootstrap_operation_id"]
        )
        bootstrap_fence = bootstrapping.data["operation_fences"][
            bootstrap_operation_id
        ]
        assert bootstrapping.state == AgentSessionState.ACTIVE_CHAT
        assert bootstrap_fence["contract_version"] == 3
        assert bootstrap_fence["handoff_operation_id"] == review_operation_id
        assert bootstrap_fence["handoff_message_log_ids"] == [review_message_log_id]
        assert bootstrap_fence["input_ledger_sequence"] == 1

        now[0] = 210.0
        assert (await executor.run_once(lane=EffectLane.PLANNER)).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="active-chat bootstrap completion",
        )
        active = await actor_store.load(key)
        assert active.active_chat_state["bootstrap_status"] == "completed"
        assert active.active_chat_state["bootstrap_disposition"] == "engaged"
        assert len(bootstrap_workflow.requests) == 1
        assert bootstrap_workflow.requests[0].handoff_message_log_ids == (
            review_message_log_id,
        )

        round_message_log_id = _insert_message(
            database,
            platform_message_id="platform-active-chat-round",
            text="continue the durable active chat",
            created_at=220.0,
        )
        now[0] = 220.0
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=round_message_log_id,
                event_id="message:active-chat-round",
                occurred_at=now[0],
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="active-chat round message",
        )

        now[0] = 221.0
        assert (await executor.run_once(lane=EffectLane.CONTROL)).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="active-chat round control effect",
        )
        round_running = await actor_store.load(key)
        round_operation_id = round_running.active_chat_round_operation_id
        round_fence = round_running.data["operation_fences"][round_operation_id]
        assert round_fence["contract_version"] == 3
        assert round_fence["message_log_ids"] == [round_message_log_id]
        assert round_fence["bootstrap_disposition"] == "engaged"

        now[0] = 230.0
        assert (await executor.run_once(lane=EffectLane.PLANNER)).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="active-chat round workflow completion",
        )
        waiting_for_receipt = await actor_store.load(key)
        pending_effect_ids = tuple(waiting_for_receipt.data["pending_outbound_actions"])
        assert len(pending_effect_ids) == 1
        assert len(round_workflow.requests) == 1
        assert round_workflow.requests[0].message_log_ids == (round_message_log_id,)
        assert round_workflow.requests[0].effect.message_log_ids == (
            round_message_log_id,
        )

        now[0] = 240.0
        assert (await executor.run_once(lane=EffectLane.DEFAULT)).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="outbound action receipt completion",
        )
        settled = await actor_store.load(key)
        assert "pending_outbound_actions" not in settled.data
        assert "outbound_continuation" not in settled.data
        assert len(adapter.sent) == 1
        target_session, elements = adapter.sent[0]
        assert target_session == "instance-a:base-session"
        assert [(element.type, element.attrs) for element in elements] == [
            ("quote", {"id": "platform-active-chat-round"}),
            ("text", {"content": "Actor v3 durable active chat reply"}),
        ]

        action_effect_id = pending_effect_ids[0]
        with database.connect() as conn:
            action_row = conn.execute(
                """
                SELECT idempotency_key FROM agent_effect_outbox WHERE effect_id = ?
                """,
                (action_effect_id,),
            ).fetchone()
        assert action_row is not None
        receipt = await receipt_store.get(key, str(action_row["idempotency_key"]))
        assert receipt is not None
        assert receipt.status is ExternalActionReceiptStatus.SUCCEEDED
        assert receipt.assistant_message_log_id is not None

        with database.connect() as conn:
            rows = conn.execute(
                """
                SELECT kind, metadata_json
                FROM agent_session_operations
                WHERE operation_id IN (?, ?, ?)
                ORDER BY operation_id
                """,
                (review_operation_id, bootstrap_operation_id, round_operation_id),
            ).fetchall()
        metadata_by_kind = {
            row["kind"]: json.loads(str(row["metadata_json"])) for row in rows
        }
        review_handoff = metadata_by_kind["review"]["active_chat_handoff"]
        bootstrap_handoff = metadata_by_kind["active_chat_bootstrap"][
            "handoff_certificate"
        ]
        assert bootstrap_handoff == review_handoff
        assert review_handoff["review_operation_id"] == review_operation_id
        assert review_handoff["message_log_ids"] == [review_message_log_id]
        assert review_handoff["source_active_epoch"] == 0
        assert review_handoff["review_consumption_id"]
        with database.connect() as conn:
            review_consumption = conn.execute(
                """
                SELECT consumption_id, idempotency_key, source_event_id,
                       explicit_message_log_ids_json
                FROM agent_message_ledger_consumptions
                WHERE operation_id = ? AND kind = 'review'
                """,
                (review_operation_id,),
            ).fetchone()
            applied_handoff_rows = conn.execute(
                """
                SELECT message_log_id
                FROM agent_message_ledger
                WHERE review_consumption_id = ?
                ORDER BY ledger_sequence
                """,
                (review_handoff["review_consumption_id"],),
            ).fetchall()
        assert review_consumption is not None
        assert review_consumption["consumption_id"] == review_handoff[
            "review_consumption_id"
        ]
        assert review_consumption["idempotency_key"] == review_handoff[
            "review_consumption_idempotency_key"
        ]
        assert review_consumption["source_event_id"] == review_handoff[
            "review_completion_event_id"
        ]
        assert json.loads(str(review_consumption["explicit_message_log_ids_json"])) == [
            review_message_log_id
        ]
        assert [row["message_log_id"] for row in applied_handoff_rows] == [
            review_message_log_id
        ]
        assert metadata_by_kind["review"]["model_execution_id"] == "review-execution-v3"
        assert metadata_by_kind["review"]["prompt_signature"] == "review-prompt-v3"
        assert metadata_by_kind["active_chat_bootstrap"]["model_execution_id"] == (
            "bootstrap-execution-v3"
        )
        assert metadata_by_kind["active_chat_bootstrap"]["prompt_signature"] == (
            "bootstrap-prompt-v3"
        )
        assert metadata_by_kind["active_chat_round"]["model_execution_id"] == (
            "round-execution-v3"
        )
        assert metadata_by_kind["active_chat_round"]["prompt_signature"] == (
            "round-prompt-v3"
        )
    finally:
        await executor.shutdown(drain=False)
        await registry.shutdown(drain=False)


@pytest.mark.asyncio
async def test_actor_v3_receipt_gated_bootstrap_keeps_frozen_review_handoff(
    tmp_path: Path,
) -> None:
    """A message during a review-reply receipt wait cannot widen bootstrap input."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot-a:instance-a:base-session")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="receipt-gated active chat bootstrap regression",
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
                active_chat_semantic_wait_seconds=0.0,
            )
        ).reduce,
        retry_delay_seconds=0.0,
        max_attempts=1,
    )
    review_workflow = _ReviewWorkflow(include_outbound_reply=True)
    bootstrap_workflow = _BootstrapWorkflow()
    round_workflow = _RoundWorkflow()
    adapter = _Adapter()
    handlers = EffectHandlerRegistry(contract_authority=authority)
    register_actor_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_reply_workflow=_NoopActiveReplyWorkflow(),
        review_workflow=review_workflow,
    )
    register_actor_active_chat_workflow_effect_handlers(
        handlers,
        ledger=actor_store,
        active_chat_bootstrap_workflow=bootstrap_workflow,
        active_chat_round_workflow=round_workflow,
    )
    register_delayed_control_effect_handlers(handlers)
    receipt_store = SQLiteExternalActionReceiptStore(database, clock=lambda: now[0])
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
        renew_interval_seconds=10.0,
        review_execution_gate_store=SQLiteReviewExecutionGateStore(
            database,
            clock=lambda: now[0],
        ),
        clock=lambda: now[0],
    )

    try:
        review_message_log_id = _insert_message(
            database,
            platform_message_id="platform-review-reply-gate",
            text="start the receipt-gated active chat handoff",
            created_at=100.0,
        )
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=review_message_log_id,
                event_id="message:review-reply-gate",
                occurred_at=100.0,
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="receipt-gated initial review message",
        )
        planned = await actor_store.load(key)

        now[0] = 150.0
        await registry.submit(
            _review_due_event(
                key=key,
                generation=ownership.generation,
                plan_id=planned.current_plan_id,
                plan_revision=planned.review_plan_revision,
                occurred_at=now[0],
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="receipt-gated review due delivery",
        )

        now[0] = 200.0
        assert (
            await executor.run_once(lane=EffectLane.PLANNER)
        ).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="receipt-gated review workflow completion",
        )
        waiting_for_receipt = await actor_store.load(key)
        assert waiting_for_receipt.state == AgentSessionState.ACTIVE_CHAT
        assert (
            waiting_for_receipt.active_chat_state["bootstrap_status"]
            == "waiting_outbound"
        )
        handoff_watermark = waiting_for_receipt.active_chat_state[
            "bootstrap_handoff_input_watermark"
        ]
        handoff_sequence = waiting_for_receipt.active_chat_state[
            "bootstrap_handoff_input_ledger_sequence"
        ]
        assert waiting_for_receipt.active_chat_state[
            "bootstrap_handoff_message_log_ids"
        ] == [review_message_log_id]
        assert handoff_watermark == review_message_log_id
        assert handoff_sequence == 1
        pending_action_effect_ids = tuple(
            waiting_for_receipt.data["pending_outbound_actions"]
        )
        assert len(pending_action_effect_ids) == 1

        later_message_log_id = _insert_message(
            database,
            platform_message_id="platform-during-review-reply-receipt",
            text="this belongs to the first active chat round, not bootstrap",
            created_at=205.0,
        )
        now[0] = 205.0
        await registry.submit(
            _message_event(
                key=key,
                generation=ownership.generation,
                message_log_id=later_message_log_id,
                event_id="message:during-review-reply-receipt",
                occurred_at=now[0],
            )
        )
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="message buffered during receipt wait",
        )
        buffered = await actor_store.load(key)
        assert buffered.active_chat_state["pending_message_log_ids"] == [
            later_message_log_id
        ]
        assert buffered.active_chat_state["bootstrap_handoff_message_log_ids"] == [
            review_message_log_id
        ]
        assert (
            buffered.active_chat_state["bootstrap_handoff_input_watermark"]
            == handoff_watermark
        )
        assert (
            buffered.active_chat_state["bootstrap_handoff_input_ledger_sequence"]
            == handoff_sequence
        )

        now[0] = 210.0
        assert (
            await executor.run_once(lane=EffectLane.DEFAULT)
        ).status is EffectRunStatus.COMPLETED
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint="receipt-gated outbound action completion",
        )

        bootstrapping = await actor_store.load(key)
        bootstrap_operation_id = str(
            bootstrapping.active_chat_state["bootstrap_operation_id"]
        )
        assert bootstrap_operation_id
        assert bootstrapping.active_chat_state["bootstrap_status"] == "pending"
        assert bootstrapping.active_chat_state["pending_message_log_ids"] == [
            later_message_log_id
        ]
        bootstrap_fence = bootstrapping.data["operation_fences"][
            bootstrap_operation_id
        ]
        assert bootstrap_fence["contract_version"] == 3
        assert bootstrap_fence["handoff_message_log_ids"] == [review_message_log_id]
        assert later_message_log_id not in bootstrap_fence["handoff_message_log_ids"]
        assert bootstrap_fence["input_watermark"] == handoff_watermark
        assert bootstrap_fence["input_ledger_sequence"] == handoff_sequence

        with database.connect() as conn:
            operation_row = conn.execute(
                """
                SELECT status, input_watermark, input_ledger_sequence, metadata_json
                FROM agent_session_operations
                WHERE operation_id = ?
                """,
                (bootstrap_operation_id,),
            ).fetchone()
            effect_row = conn.execute(
                """
                SELECT kind, contract_version, status, payload_json
                FROM agent_effect_outbox
                WHERE operation_id = ?
                """,
                (bootstrap_operation_id,),
            ).fetchone()
        assert operation_row is not None
        assert operation_row["status"] == "pending"
        assert operation_row["input_watermark"] == handoff_watermark
        assert operation_row["input_ledger_sequence"] == handoff_sequence
        handoff_certificate = bootstrapping.active_chat_state[
            "bootstrap_handoff_certificate"
        ]
        assert json.loads(str(operation_row["metadata_json"]))[
            "handoff_certificate"
        ] == handoff_certificate
        assert effect_row is not None
        assert effect_row["kind"] == "run_active_chat_bootstrap"
        assert effect_row["contract_version"] == 3
        assert effect_row["status"] == "pending"
        queued_payload = json.loads(str(effect_row["payload_json"]))
        assert queued_payload["handoff_message_log_ids"] == [review_message_log_id]
        assert later_message_log_id not in queued_payload["handoff_message_log_ids"]
        assert queued_payload["input_watermark"] == handoff_watermark
        assert queued_payload["input_ledger_sequence"] == handoff_sequence

        action_effect_id = pending_action_effect_ids[0]
        idempotency_key = waiting_for_receipt.data["pending_outbound_actions"][
            action_effect_id
        ]["idempotency_key"]
        receipt = await receipt_store.get(key, idempotency_key)
        assert receipt is not None
        assert receipt.status is ExternalActionReceiptStatus.SUCCEEDED
        actor = registry.actor_for(key)
        assert actor is not None
        assert actor.last_error is None
    finally:
        await executor.shutdown(drain=False)
        await registry.shutdown(drain=False)
