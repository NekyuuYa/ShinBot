"""Tests for shinbot.core.pipeline — MessageContext and MessagePipeline."""

import pytest

from shinbot.core.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.command import CommandDef, CommandRegistry
from shinbot.core.event_bus import EventBus
from shinbot.core.permission import PermissionEngine
from shinbot.core.pipeline import MessageContext, MessagePipeline
from shinbot.core.session import Session, SessionConfig, SessionManager
from shinbot.models.elements import Message, MessageElement
from shinbot.models.events import Channel, MessagePayload, UnifiedEvent, User


# ── Mock adapter for testing ─────────────────────────────────────────


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id="test-bot", platform="mock", **kwargs):
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


class TestMessageContext:
    def setup_method(self):
        self.adapter = MockAdapter()
        self.event = make_event("hello world")
        self.message = Message.from_text("hello world")
        self.session = Session(
            id="test-bot:private:user-1",
            instance_id="test-bot",
            session_type="private",
        )
        self.ctx = MessageContext(
            event=self.event,
            message=self.message,
            session=self.session,
            adapter=self.adapter,
            permissions={"cmd.help", "cmd.ping"},
        )

    def test_text(self):
        assert self.ctx.text == "hello world"

    def test_user_id(self):
        assert self.ctx.user_id == "user-1"

    def test_session_id(self):
        assert self.ctx.session_id == "test-bot:private:user-1"

    def test_is_private(self):
        assert self.ctx.is_private is True

    def test_has_permission(self):
        assert self.ctx.has_permission("cmd.help") is True
        assert self.ctx.has_permission("sys.reboot") is False

    def test_elapsed_ms(self):
        assert self.ctx.elapsed_ms >= 0

    @pytest.mark.asyncio
    async def test_send_text(self):
        handle = await self.ctx.send("response text")
        assert len(self.adapter.sent) == 1
        assert handle.message_id == "sent-1"

    @pytest.mark.asyncio
    async def test_send_xml(self):
        handle = await self.ctx.send('hi <at id="123"/>')
        assert len(self.adapter.sent) == 1
        elements = self.adapter.sent[0][1]
        assert len(elements) == 2  # text + at

    @pytest.mark.asyncio
    async def test_send_message_object(self):
        msg = Message.from_elements(MessageElement.text("test"))
        await self.ctx.send(msg)
        assert len(self.adapter.sent) == 1

    @pytest.mark.asyncio
    async def test_send_element_list(self):
        els = [MessageElement.text("a"), MessageElement.text("b")]
        await self.ctx.send(els)
        assert len(self.adapter.sent) == 1
        assert len(self.adapter.sent[0][1]) == 2

    @pytest.mark.asyncio
    async def test_reply(self):
        handle = await self.ctx.reply("reply text")
        assert len(self.adapter.sent) == 1
        elements = self.adapter.sent[0][1]
        assert elements[0].type == "quote"

    def test_stop(self):
        assert self.ctx.is_stopped is False
        self.ctx.stop()
        assert self.ctx.is_stopped is True


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
    async def test_non_message_event(self):
        """Non-message events should go to event bus directly."""
        results = []

        async def handler(ctx):
            results.append(ctx.event.type)

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
