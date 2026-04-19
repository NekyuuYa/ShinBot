from __future__ import annotations

import json

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState
from shinbot.agent.attention.tools import register_attention_tools
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.agent.workflow import WorkflowRunner
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import Session, SessionManager
from shinbot.persistence import (
    AgentRecord,
    BotConfigRecord,
    DatabaseManager,
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

    async def start(self):
        pass

    async def shutdown(self):
        pass

    async def send(self, target_session, elements):
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method, params):
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
    assert [elements[0].text_content for _, elements in adapter.sent] == ["第一条回复", "第二条回复"]
