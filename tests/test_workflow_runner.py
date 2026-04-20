from __future__ import annotations

import json

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState
from shinbot.agent.attention.tools import register_attention_tools
from shinbot.agent.context import ContextManager
from shinbot.agent.identity import IdentityStore, register_identity_prompt_components
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.runtime import register_runtime_prompt_components
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.agent.workflow import WorkflowRunner
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import Session, SessionManager
from shinbot.persistence import (
    AgentRecord,
    BotConfigRecord,
    DatabaseManager,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)
from shinbot.persistence.records import utc_now_iso
from shinbot.schema.elements import MessageElement


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "inst-workflow", platform: str = "mock"):
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.api_calls: list[tuple[str, dict[str, object]]] = []

    async def start(self):
        pass

    async def shutdown(self):
        pass

    async def send(self, target_session, elements):
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method, params):
        self.api_calls.append((method, params))
        return {"ok": True}

    async def get_capabilities(self):
        return {"elements": ["text"], "actions": [], "limits": {}}


class QueuedModelRuntime:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.calls: list[object] = []

    async def generate(self, call):
        self.calls.append(call)
        if not self._responses:
            raise AssertionError("No queued workflow response left")
        response = self._responses.pop(0)
        return type(
            "Result",
            (),
            {
                "text": str(response.get("text", "")),
                "tool_calls": list(response.get("tool_calls", [])),
                "execution_id": f"exec-{len(self.calls)}",
                "route_id": call.route_id or "",
                "provider_id": "provider",
                "model_id": call.model_id or "",
                "usage": {},
            },
        )()


def _seed_workflow_runtime(db: DatabaseManager, *, instance_id: str) -> None:
    now = utc_now_iso()
    provider_id = "openai-workflow"
    model_id = "openai-workflow/gpt-test"
    route_id = "route.workflow.test"
    prompt_uuid = "prompt-workflow-test"
    persona_uuid = "persona-workflow-test"
    agent_uuid = "agent-workflow-test-uuid"

    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type="openai",
            display_name="OpenAI Workflow",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model="openai/gpt-4.1-mini",
            display_name="Workflow Test Model",
            capabilities=["chat"],
            context_window=32000,
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=model_id,
                priority=10,
                weight=1.0,
            )
        ],
    )
    db.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid=prompt_uuid,
            prompt_id="prompt.workflow.test",
            name="Workflow Persona",
            source_type="agent_plugin",
            source_id=agent_uuid,
            stage="identity",
            type="static_text",
            priority=100,
            enabled=True,
            content="You are a workflow agent.",
            created_at=now,
            updated_at=now,
        )
    )
    db.personas.upsert(
        PersonaRecord(
            uuid=persona_uuid,
            name="Workflow Persona",
            prompt_definition_uuid=prompt_uuid,
            created_at=now,
            updated_at=now,
        )
    )
    db.agents.upsert(
        AgentRecord(
            uuid=agent_uuid,
            agent_id="agent.workflow.test",
            name="Workflow Agent",
            persona_uuid=persona_uuid,
            created_at=now,
            updated_at=now,
        )
    )
    db.bot_configs.upsert(
        BotConfigRecord(
            uuid="bot-config-workflow-test",
            instance_id=instance_id,
            default_agent_uuid=agent_uuid,
            main_llm=route_id,
            created_at=now,
            updated_at=now,
        )
    )


@pytest.mark.asyncio
async def test_workflow_runner_continues_after_send_reply_when_not_terminating(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    instance_id = "inst-workflow"
    session_id = f"{instance_id}:group:1"
    _seed_workflow_runtime(db, instance_id=instance_id)
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id=instance_id,
            session_type="group",
            platform="mock",
            channel_id="1",
        )
    )

    attention_engine = AttentionEngine(AttentionConfig(), db.attention)
    attention_state = SessionAttentionState(
        session_id=session_id,
        attention_value=6.0,
        last_consumed_msg_log_id=1,
        last_trigger_msg_log_id=1,
        metadata={"unanswered_mention_streak": 3},
    )
    attention_engine.repo.save_attention(attention_state)

    adapter = MockAdapter(instance_id=instance_id)
    adapter_manager = AdapterManager()
    adapter_manager._instances[instance_id] = adapter

    registry = ToolRegistry()
    register_attention_tools(registry, attention_engine, adapter_manager, db)
    tool_manager = ToolManager(registry, permission_engine=PermissionEngine())

    runtime = QueuedModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {
                            "name": "send_reply",
                            "arguments": json.dumps(
                                {"text": "第一条回复", "terminate_round": False},
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            },
            {
                "tool_calls": [
                    {
                        "id": "call-2",
                        "type": "function",
                        "function": {
                            "name": "send_reply",
                            "arguments": json.dumps(
                                {"text": "第二条回复", "terminate_round": True},
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            },
        ]
    )

    runner = WorkflowRunner(
        db,
        PromptRegistry(),
        runtime,
        tool_manager,
        attention_engine,
        adapter_manager,
    )

    batch = [
        {
            "id": 1,
            "session_id": session_id,
            "platform_msg_id": "msg-1",
            "sender_id": "user-1",
            "sender_name": "Tester",
            "raw_text": "你好",
            "is_mentioned": 0,
            "content_json": json.dumps(
                [MessageElement.text("你好").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        }
    ]

    record = await runner.run(
        session_id,
        batch,
        attention_state,
        instance_id=instance_id,
    )

    assert record is not None
    assert record.replied is True
    assert record.response_summary == "第二条回复"
    assert len(runtime.calls) == 2
    assert [elements[0].text_content for _, elements in adapter.sent] == [
        "第一条回复",
        "第二条回复",
    ]
    refreshed = attention_engine.repo.get_attention(session_id)
    assert refreshed is not None
    assert refreshed.metadata.get("unanswered_mention_streak") == 0


@pytest.mark.asyncio
async def test_workflow_runner_send_reply_can_quote_platform_message(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    instance_id = "inst-workflow"
    session_id = f"{instance_id}:group:1"
    _seed_workflow_runtime(db, instance_id=instance_id)
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id=instance_id,
            session_type="group",
            platform="mock",
            channel_id="1",
        )
    )

    attention_engine = AttentionEngine(AttentionConfig(), db.attention)
    attention_state = SessionAttentionState(
        session_id=session_id,
        attention_value=6.0,
        last_consumed_msg_log_id=1,
        last_trigger_msg_log_id=1,
    )
    attention_engine.repo.save_attention(attention_state)

    adapter = MockAdapter(instance_id=instance_id)
    adapter_manager = AdapterManager()
    adapter_manager._instances[instance_id] = adapter

    registry = ToolRegistry()
    register_attention_tools(registry, attention_engine, adapter_manager, db)
    tool_manager = ToolManager(registry, permission_engine=PermissionEngine())

    runtime = QueuedModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-quote",
                        "type": "function",
                        "function": {
                            "name": "send_reply",
                            "arguments": json.dumps(
                                {
                                    "text": "引用这条回复",
                                    "quote_message_id": "msg-to-quote",
                                },
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            }
        ]
    )

    runner = WorkflowRunner(
        db,
        PromptRegistry(),
        runtime,
        tool_manager,
        attention_engine,
        adapter_manager,
    )
    batch = [
        {
            "id": 1,
            "session_id": session_id,
            "platform_msg_id": "msg-to-quote",
            "sender_id": "user-1",
            "sender_name": "Tester",
            "raw_text": "请引用我",
            "is_mentioned": 0,
            "content_json": json.dumps(
                [MessageElement.text("请引用我").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        }
    ]

    record = await runner.run(
        session_id,
        batch,
        attention_state,
        instance_id=instance_id,
    )

    assert record is not None
    assert record.replied is True
    assert len(adapter.sent) == 1
    sent_elements = adapter.sent[0][1]
    assert sent_elements[0] == MessageElement.quote("msg-to-quote")
    assert sent_elements[1] == MessageElement.text("引用这条回复")

    assistant_rows = [
        row for row in db.message_logs.list_by_session(session_id) if row["role"] == "assistant"
    ]
    assert len(assistant_rows) == 1
    persisted_elements = json.loads(assistant_rows[0]["content_json"])
    assert persisted_elements[0]["type"] == "quote"
    assert persisted_elements[0]["attrs"]["id"] == "msg-to-quote"


@pytest.mark.asyncio
async def test_workflow_runner_uses_single_user_message_for_batch_prompt(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    instance_id = "inst-workflow"
    session_id = f"{instance_id}:group:1"
    _seed_workflow_runtime(db, instance_id=instance_id)
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id=instance_id,
            session_type="group",
            platform="qq",
            channel_id="1",
        )
    )

    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="旧上下文，应该作为历史 context 进来",
            sender_id="legacy-user",
            sender_name="Legacy",
            created_at=1000,
            content_json="[]",
            platform_msg_id="legacy-1",
            is_read=True,
        )
    )

    attention_engine = AttentionEngine(AttentionConfig(), db.attention)
    attention_state = SessionAttentionState(
        session_id=session_id,
        attention_value=6.0,
        last_consumed_msg_log_id=10,
        last_trigger_msg_log_id=10,
    )
    attention_engine.repo.save_attention(attention_state)

    adapter = MockAdapter(instance_id=instance_id, platform="qq")
    adapter_manager = AdapterManager()
    adapter_manager._instances[instance_id] = adapter

    registry = ToolRegistry()
    register_attention_tools(registry, attention_engine, adapter_manager, db)
    tool_manager = ToolManager(registry, permission_engine=PermissionEngine())

    identity_store = IdentityStore(tmp_path / "identities.json")
    context_manager = ContextManager(db.message_logs, identity_store=identity_store)
    prompt_registry = PromptRegistry(
        context_manager=context_manager,
        identity_store=identity_store,
    )
    register_identity_prompt_components(
        prompt_registry,
        resolver=prompt_registry.resolve_builtin_identity_map_prompt,
    )
    register_runtime_prompt_components(
        prompt_registry,
        message_text_resolver=prompt_registry.resolve_builtin_message_text_prompt,
        current_time_resolver=prompt_registry.resolve_builtin_current_time_prompt,
    )

    runtime = QueuedModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {
                            "name": "send_reply",
                            "arguments": json.dumps(
                                {"text": "收到", "terminate_round": True},
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            }
        ]
    )

    runner = WorkflowRunner(
        db,
        prompt_registry,
        runtime,
        tool_manager,
        attention_engine,
        adapter_manager,
    )

    batch = [
        {
            "id": 11,
            "session_id": session_id,
            "platform_msg_id": "msg-11",
            "sender_id": "602190328",
            "sender_name": "UNOwen",
            "raw_text": "或许要攒够5条",
            "is_mentioned": 0,
            "created_at": 1_000.0,
            "content_json": json.dumps(
                [MessageElement.text("或许要攒够5条").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        },
        {
            "id": 12,
            "session_id": session_id,
            "platform_msg_id": "msg-12",
            "sender_id": "1917419834",
            "sender_name": "Ginkoro",
            "raw_text": "但是at是有特殊权重的",
            "is_mentioned": 0,
            "created_at": 2_000.0,
            "content_json": json.dumps(
                [MessageElement.text("但是at是有特殊权重的").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        },
    ]

    record = await runner.run(
        session_id,
        batch,
        attention_state,
        instance_id=instance_id,
    )

    assert record is not None
    assert record.replied is True
    assert len(runtime.calls) == 1

    call_messages = runtime.calls[0].messages
    initial_messages = call_messages[:3]
    assert [message["role"] for message in initial_messages] == ["system", "user", "user"]
    assert "name" not in initial_messages[0]
    assert "旧上下文，应该作为历史 context 进来" in str(initial_messages[1]["content"])
    assert "name" not in initial_messages[2]

    final_user_message = initial_messages[-1]
    final_texts = [str(block.get("text", "")) for block in final_user_message["content"]]

    assert final_texts[0] == "[以下是会话中 2 条未消费消息]"
    assert "UNOwen: 或许要攒够5条" in final_texts[1]
    assert "message_log_id=11" in final_texts[1]
    assert "platform_msg_id=msg-11" in final_texts[1]
    assert "时间: " in final_texts[1]
    assert "Ginkoro: 但是at是有特殊权重的" in final_texts[2]
    assert "message_log_id=12" in final_texts[2]
    assert "platform_msg_id=msg-12" in final_texts[2]
    assert "时间: " in final_texts[2]
    assert not any(
        "UNOwen: 或许要攒够5条" in text and "Ginkoro: 但是at是有特殊权重的" in text
        for text in final_texts
    )
    assert any("### 参与者身份参考 (Identity Map)" in text for text in final_texts)
    assert any("ID: 602190328 -> 昵称: UNOwen" in text for text in final_texts)
    assert any("ID: 1917419834 -> 昵称: Ginkoro" in text for text in final_texts)
    assert any("### 行为约束" in text for text in final_texts)
    assert any("### 当前时间" in text for text in final_texts)
    assert not any("旧上下文" in text for text in final_texts)


@pytest.mark.asyncio
async def test_workflow_runner_poke_tool_counts_as_visible_action(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    instance_id = "inst-workflow"
    session_id = f"{instance_id}:group:1"
    _seed_workflow_runtime(db, instance_id=instance_id)
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id=instance_id,
            session_type="group",
            platform="mock",
            channel_id="1",
        )
    )

    attention_engine = AttentionEngine(AttentionConfig(), db.attention)
    attention_state = SessionAttentionState(
        session_id=session_id,
        attention_value=6.0,
        last_consumed_msg_log_id=1,
        last_trigger_msg_log_id=1,
    )
    attention_engine.repo.save_attention(attention_state)

    adapter = MockAdapter(instance_id=instance_id)
    adapter_manager = AdapterManager()
    adapter_manager._instances[instance_id] = adapter

    registry = ToolRegistry()
    register_attention_tools(registry, attention_engine, adapter_manager, db)
    tool_manager = ToolManager(registry, permission_engine=PermissionEngine())

    runtime = QueuedModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-poke",
                        "type": "function",
                        "function": {
                            "name": "send_poke",
                            "arguments": json.dumps(
                                {"user_id": "user-2", "terminate_round": True},
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            }
        ]
    )

    runner = WorkflowRunner(
        db,
        PromptRegistry(),
        runtime,
        tool_manager,
        attention_engine,
        adapter_manager,
    )

    batch = [
        {
            "id": 1,
            "session_id": session_id,
            "platform_msg_id": "msg-1",
            "sender_id": "user-1",
            "sender_name": "Tester",
            "raw_text": "戳一下他",
            "is_mentioned": 0,
            "content_json": json.dumps(
                [MessageElement.text("戳一下他").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        }
    ]

    record = await runner.run(
        session_id,
        batch,
        attention_state,
        instance_id=instance_id,
    )

    assert record is not None
    assert record.replied is True
    assert record.response_summary == "戳一戳"
    assert adapter.api_calls == [("internal.mock.poke", {"user_id": "user-2", "group_id": "1"})]


@pytest.mark.asyncio
async def test_workflow_runner_resets_mention_streak_on_no_reply(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    instance_id = "inst-workflow"
    session_id = f"{instance_id}:group:1"
    _seed_workflow_runtime(db, instance_id=instance_id)
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id=instance_id,
            session_type="group",
            platform="mock",
            channel_id="1",
        )
    )

    attention_engine = AttentionEngine(AttentionConfig(), db.attention)
    attention_state = SessionAttentionState(
        session_id=session_id,
        attention_value=6.0,
        last_consumed_msg_log_id=1,
        last_trigger_msg_log_id=1,
        metadata={"unanswered_mention_streak": 5},
    )
    attention_engine.repo.save_attention(attention_state)

    adapter = MockAdapter(instance_id=instance_id)
    adapter_manager = AdapterManager()
    adapter_manager._instances[instance_id] = adapter

    registry = ToolRegistry()
    register_attention_tools(registry, attention_engine, adapter_manager, db)
    tool_manager = ToolManager(registry, permission_engine=PermissionEngine())

    runtime = QueuedModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "call-no-reply",
                        "type": "function",
                        "function": {
                            "name": "no_reply",
                            "arguments": json.dumps(
                                {"internal_summary": "keep sleeping"},
                                ensure_ascii=False,
                            ),
                        },
                    }
                ]
            }
        ]
    )

    runner = WorkflowRunner(
        db,
        PromptRegistry(),
        runtime,
        tool_manager,
        attention_engine,
        adapter_manager,
    )

    batch = [
        {
            "id": 1,
            "session_id": session_id,
            "platform_msg_id": "msg-1",
            "sender_id": "user-1",
            "sender_name": "Tester",
            "raw_text": "@bot",
            "is_mentioned": 1,
            "content_json": json.dumps(
                [MessageElement.text("@bot").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        }
    ]

    record = await runner.run(
        session_id,
        batch,
        attention_state,
        instance_id=instance_id,
    )

    assert record is not None
    assert record.replied is False

    refreshed = attention_engine.repo.get_attention(session_id)
    assert refreshed is not None
    assert refreshed.metadata.get("unanswered_mention_streak") == 0
    assert refreshed.metadata.get("internal_summary") == "keep sleeping"
