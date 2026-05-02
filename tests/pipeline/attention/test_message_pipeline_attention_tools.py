"""Attention tool integration tests."""

import json
import time

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.tools import register_attention_tools
from shinbot.agent.context import ContextManager
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    MessageLogRecord,
)
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User
from shinbot.utils.resource_ingress import summarize_message_modalities

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# ── Mock adapter for testing ─────────────────────────────────────────


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id="test-bot", platform="mock", **kwargs):
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.api_calls: list[tuple[str, dict]] = []

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


# ── Fixtures ─────────────────────────────────────────────────────────


def make_event(content="hello", user_id="user-1", channel_type=1):
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id),
        channel=Channel(
            id=f"private:{user_id}" if channel_type == 1 else "group:1", type=channel_type
        ),
        message=MessagePayload(id="msg-1", content=content),
    )


class TestAttentionTools:
    def setup_method(self):
        self.adapter_mgr = AdapterManager()
        self.adapter_mgr.register_adapter("mock", MockAdapter)
        self.perm_engine = PermissionEngine()
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_attention_send_reply_tool_persists_message_log(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        context_manager = ContextManager(db.message_logs)
        self.adapter_mgr._instances[self.adapter.instance_id] = self.adapter
        registry = ToolRegistry()
        manager = ToolManager(registry, permission_engine=self.perm_engine)
        register_attention_tools(
            registry,
            AttentionEngine(AttentionConfig(), db.attention),
            self.adapter_mgr,
            db,
            context_manager,
        )

        result = await manager.execute(
            ToolCallRequest(
                tool_name="send_reply",
                arguments={"text": "workflow reply"},
                caller="attention.workflow_runner",
                instance_id=self.adapter.instance_id,
                session_id="test-bot:private:user-1",
            )
        )

        assert result.success is True
        assert len(self.adapter.sent) == 1
        assert result.output["message_log_id"] is not None
        assert result.output["terminate_round"] is True

        row = db.message_logs.get(result.output["message_log_id"])
        assert row is not None
        assert row["role"] == "assistant"
        assert row["raw_text"] == "workflow reply"

        turns = context_manager.get_context_inputs("test-bot:private:user-1")["history_turns"]
        assert turns[-1]["content"] == "workflow reply"

    @pytest.mark.asyncio
    async def test_attention_send_reply_tool_can_quote_message_log(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        self.adapter_mgr._instances[self.adapter.instance_id] = self.adapter
        registry = ToolRegistry()
        manager = ToolManager(registry, permission_engine=self.perm_engine)
        register_attention_tools(
            registry,
            AttentionEngine(AttentionConfig(), db.attention),
            self.adapter_mgr,
            db,
        )
        quoted_log_id = db.message_logs.insert(
            MessageLogRecord(
                session_id="test-bot:private:user-1",
                platform_msg_id="quoted-platform-msg",
                sender_id="user-1",
                sender_name="Tester",
                content_json="[]",
                raw_text="please answer this",
                role="user",
                is_read=True,
                is_mentioned=False,
                created_at=time.time() * 1000,
            )
        )

        result = await manager.execute(
            ToolCallRequest(
                tool_name="send_reply",
                arguments={
                    "text": "workflow quoted reply",
                    "quote_message_log_id": quoted_log_id,
                },
                caller="attention.workflow_runner",
                instance_id=self.adapter.instance_id,
                session_id="test-bot:private:user-1",
            )
        )

        assert result.success is True
        assert result.output["quote_message_id"] == "quoted-platform-msg"
        elements = self.adapter.sent[0][1]
        assert elements[0] == MessageElement.quote("quoted-platform-msg")
        assert elements[1] == MessageElement.text("workflow quoted reply")

        row = db.message_logs.get(result.output["message_log_id"])
        assert row is not None
        persisted_elements = json.loads(row["content_json"])
        assert persisted_elements[0]["type"] == "quote"
        assert persisted_elements[0]["attrs"]["id"] == "quoted-platform-msg"

    @pytest.mark.asyncio
    async def test_attention_poke_tools_call_internal_api(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        self.adapter_mgr._instances[self.adapter.instance_id] = self.adapter
        registry = ToolRegistry()
        manager = ToolManager(registry, permission_engine=self.perm_engine)
        register_attention_tools(
            registry,
            AttentionEngine(AttentionConfig(), db.attention),
            self.adapter_mgr,
            db,
        )

        exported_names = {
            str(tool.get("function", {}).get("name", ""))
            for tool in manager.export_model_tools(
                caller="attention.workflow_runner",
                instance_id=self.adapter.instance_id,
                session_id="test-bot:group:group:1",
                tags={"attention"},
            )
        }
        assert "send_poke" in exported_names

        result = await manager.execute(
            ToolCallRequest(
                tool_name="send_poke",
                arguments={"user_id": "user-2"},
                caller="attention.workflow_runner",
                instance_id=self.adapter.instance_id,
                session_id="test-bot:group:group:1",
            )
        )

        assert result.success is True
        assert result.output["terminate_round"] is True
        assert self.adapter.api_calls[-1] == (
            "internal.mock.poke",
            {"user_id": "user-2", "group_id": "1"},
        )

        result_no_terminate = await manager.execute(
            ToolCallRequest(
                tool_name="send_poke",
                arguments={"user_id": "user-3", "terminate_round": False},
                caller="attention.workflow_runner",
                instance_id=self.adapter.instance_id,
                session_id="test-bot:private:user-1",
            )
        )

        assert result_no_terminate.success is True
        assert result_no_terminate.output["terminate_round"] is False
        assert self.adapter.api_calls[-1] == (
            "internal.mock.poke",
            {"user_id": "user-3"},
        )

    def test_audit_message_modality_summary(self, tmp_path):
        audit = AuditLogger(tmp_path)
        summary = summarize_message_modalities(
            [
                MessageElement.text("hello"),
                MessageElement.img("/tmp/image.png"),
                MessageElement.audio("/tmp/audio.ogg"),
            ]
        )

        entry = audit.log_message(
            event_type="message-created",
            plugin_id="",
            user_id="user-1",
            session_id="session-1",
            instance_id="bot-1",
            metadata={"modality": summary},
        )

        assert entry.entry_type == "message"
        assert entry.metadata["modality"]["counts"]["text"] == 1
        assert entry.metadata["modality"]["counts"]["image"] == 1
        assert entry.metadata["modality"]["counts"]["audio"] == 1
