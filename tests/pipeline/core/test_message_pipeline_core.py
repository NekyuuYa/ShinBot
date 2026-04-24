"""Tests for message pipeline dispatch."""

import asyncio
import time

import pytest

from shinbot.core.dispatch.command import CommandDef, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.pipeline import MessagePipeline
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    MessageLogRecord,
)
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User

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


class TestMessagePipeline:
    def setup_method(self):
        self.adapter_mgr = AdapterManager()
        self.adapter_mgr.register_adapter("mock", MockAdapter)
        self.session_mgr = SessionManager()
        self.perm_engine = PermissionEngine()
        self.cmd_registry = CommandRegistry()
        self.event_bus = EventBus()
        self.pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
        )
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_basic_event_processing(self):
        """Event should create a session and emit to event bus."""
        results = []

        async def handler(ctx):
            results.append(ctx.text)

        self.event_bus.on("message-created", handler)

        event = make_event("hello")
        await self.pipeline.process_event(event, self.adapter)

        assert results == ["hello"]
        assert len(self.session_mgr) == 1

    @pytest.mark.asyncio
    async def test_command_dispatch(self):
        """Commands should be resolved and dispatched to handler."""
        results = []

        async def ping_handler(ctx, args):
            results.append(f"pong {args}")

        cmd = CommandDef(name="ping", handler=ping_handler)
        self.cmd_registry.register(cmd)

        event = make_event("/ping 123")
        await self.pipeline.process_event(event, self.adapter)

        assert results == ["pong 123"]

    @pytest.mark.asyncio
    async def test_command_not_in_event_bus(self):
        """When a command matches, event bus should NOT be triggered."""
        bus_results = []

        async def bus_handler(ctx):
            bus_results.append(True)

        self.event_bus.on("message-created", bus_handler)

        async def cmd_handler(ctx, args):
            pass

        self.cmd_registry.register(CommandDef(name="ping", handler=cmd_handler))

        event = make_event("/ping")
        await self.pipeline.process_event(event, self.adapter)

        assert bus_results == []

    @pytest.mark.asyncio
    async def test_interceptor_blocks(self):
        """Interceptor returning False should block processing."""
        results = []

        async def blocker(ctx):
            return False

        async def handler(ctx):
            results.append(True)

        self.pipeline.add_interceptor(blocker)
        self.event_bus.on("message-created", handler)

        event = make_event("hello")
        await self.pipeline.process_event(event, self.adapter)

        assert results == []

    @pytest.mark.asyncio
    async def test_interceptor_allows(self):
        """Interceptor returning True should allow processing."""
        results = []

        async def allower(ctx):
            return True

        async def handler(ctx):
            results.append(True)

        self.pipeline.add_interceptor(allower)
        self.event_bus.on("message-created", handler)

        event = make_event("hello")
        await self.pipeline.process_event(event, self.adapter)

        assert results == [True]

    @pytest.mark.asyncio
    async def test_muted_session_skips(self):
        """Muted sessions should not process messages."""
        results = []

        async def handler(ctx):
            results.append(True)

        self.event_bus.on("message-created", handler)

        event = make_event("hello")
        # Pre-create a muted session
        session = self.session_mgr.get_or_create(self.adapter.instance_id, event)
        session.config.is_muted = True

        await self.pipeline.process_event(event, self.adapter)
        assert results == []

    @pytest.mark.asyncio
    async def test_command_permission_denied(self):
        """Commands with insufficient permissions should be rejected."""
        results = []

        async def secret_handler(ctx, args):
            results.append(True)

        cmd = CommandDef(name="secret", handler=secret_handler, permission="admin.secret")
        self.cmd_registry.register(cmd)

        event = make_event("/secret")
        await self.pipeline.process_event(event, self.adapter)

        assert results == []
        # Should have sent a permission denied message
        assert len(self.adapter.sent) == 1

    @pytest.mark.asyncio
    async def test_attention_scheduler_receives_reply_to_bot_flag(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        session_id = "test-bot:group:group:1"
        db.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                platform_msg_id="bot-msg-1",
                sender_id="bot-1",
                sender_name="Bot",
                content_json="[]",
                raw_text="earlier bot reply",
                role="assistant",
                is_read=True,
                is_mentioned=False,
                created_at=time.time() * 1000,
            )
        )

        class RecordingAttentionScheduler:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            async def on_message(
                self,
                session_id: str,
                msg_log_id: int,
                sender_id: str,
                *,
                response_profile: str = "balanced",
                is_mentioned: bool = False,
                is_reply_to_bot: bool = False,
                attention_multiplier: float = 1.0,
                self_platform_id: str = "",
            ) -> None:
                self.calls.append(
                    {
                        "session_id": session_id,
                        "msg_log_id": msg_log_id,
                        "sender_id": sender_id,
                        "response_profile": response_profile,
                        "is_mentioned": is_mentioned,
                        "is_reply_to_bot": is_reply_to_bot,
                        "attention_multiplier": attention_multiplier,
                        "self_platform_id": self_platform_id,
                    }
                )

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        event = make_event('<quote id="bot-msg-1"/>follow-up', channel_type=0)
        await pipeline.process_event(event, self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["is_reply_to_bot"] is True
        assert scheduler.calls[0]["self_platform_id"] == "bot-1"

    @pytest.mark.asyncio
    async def test_non_message_event(self):
        """Non-message events should go to event bus directly with UnifiedEvent."""
        results = []

        async def handler(event):
            # Notice event handlers receive UnifiedEvent directly, not MessageContext
            results.append(event.type)

        self.event_bus.on("member-joined", handler)

        event = UnifiedEvent(
            type="member-joined",
            platform="mock",
            user=User(id="user-1"),
            channel=Channel(id="group:1", type=0),
        )
        await self.pipeline.process_event(event, self.adapter)
        assert results == ["member-joined"]

    @pytest.mark.asyncio
    async def test_empty_message_content(self):
        """Events with empty message content should still process."""
        results = []

        async def handler(ctx):
            results.append(len(ctx.elements))

        self.event_bus.on("message-created", handler)

        event = UnifiedEvent(
            type="message-created",
            platform="mock",
            user=User(id="user-1"),
            channel=Channel(id="private:user-1", type=1),
            message=MessagePayload(id="msg-1", content=""),
        )
        await self.pipeline.process_event(event, self.adapter)
        assert results == [0]

