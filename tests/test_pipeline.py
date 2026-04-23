"""Tests for message pipeline dispatch."""

import asyncio
import json
import time

import pytest
from PIL import Image

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.tools import register_attention_tools
from shinbot.agent.context import ContextManager
from shinbot.agent.identity import IdentityStore
from shinbot.agent.media import MediaService
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.core.dispatch.command import CommandDef, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.pipeline import MessageContext, MessagePipeline
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import Session, SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    BotConfigRecord,
    MessageLogRecord,
)
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, Member, User
from shinbot.utils.resource_ingress import summarize_message_modalities

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
        await self.ctx.send('hi <at id="123"/>')
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
        await self.ctx.reply("reply text")
        assert len(self.adapter.sent) == 1
        elements = self.adapter.sent[0][1]
        assert elements[0].type == "quote"

    @pytest.mark.asyncio
    async def test_kick_uses_event_guild_id(self):
        event = self.event.model_copy(update={"guild": Guild(id="guild-1")})
        bot = MessageContext(
            event=event,
            message=self.message,
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
        )
        await bot.kick("user-2")
        assert self.adapter.api_calls[-1] == (
            "member.kick",
            {"user_id": "user-2", "guild_id": "guild-1"},
        )

    @pytest.mark.asyncio
    async def test_kick_requires_guild_id(self):
        with pytest.raises(ValueError, match="guild_id is required"):
            await self.ctx.kick("user-2")

    @pytest.mark.asyncio
    async def test_mute_api_failure_raises_clear_error(self, monkeypatch):
        event = self.event.model_copy(update={"guild": Guild(id="guild-1")})
        bot = MessageContext(
            event=event,
            message=self.message,
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
        )

        async def _broken_call_api(method, params):
            raise RuntimeError("adapter failure")

        monkeypatch.setattr(self.adapter, "call_api", _broken_call_api)
        with pytest.raises(RuntimeError, match="API call failed: member.mute"):
            await bot.mute("user-2", duration=60)

    @pytest.mark.asyncio
    async def test_poke_calls_internal_namespace(self):
        await self.ctx.poke("user-2")
        assert self.adapter.api_calls[-1] == (
            "internal.mock.poke",
            {"user_id": "user-2"},
        )

    @pytest.mark.asyncio
    async def test_approve_friend_calls_api(self):
        await self.ctx.approve_friend("msg-123")
        assert self.adapter.api_calls[-1] == (
            "friend.approve",
            {"message_id": "msg-123"},
        )

    @pytest.mark.asyncio
    async def test_get_member_list_uses_guild_id(self):
        event = self.event.model_copy(update={"guild": Guild(id="guild-1")})
        bot = MessageContext(
            event=event,
            message=self.message,
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
        )
        await bot.get_member_list()
        assert self.adapter.api_calls[-1] == (
            "guild.member.list",
            {"guild_id": "guild-1"},
        )

    @pytest.mark.asyncio
    async def test_set_group_name_falls_back_to_internal(self, monkeypatch):
        event = self.event.model_copy(update={"guild": Guild(id="guild-1")})
        bot = MessageContext(
            event=event,
            message=self.message,
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
        )

        async def _call_api(method, params):
            if method == "guild.update":
                raise RuntimeError("not supported")
            self.adapter.api_calls.append((method, params))
            return {"ok": True}

        monkeypatch.setattr(self.adapter, "call_api", _call_api)
        await bot.set_group_name("New Name")
        assert self.adapter.api_calls[-1] == (
            "internal.mock.set_group_name",
            {"group_id": "guild-1", "group_name": "New Name"},
        )

    @pytest.mark.asyncio
    async def test_delete_msg_calls_api(self):
        await self.ctx.delete_msg("msg-42")
        assert self.adapter.api_calls[-1] == (
            "message.delete",
            {"message_id": "msg-42"},
        )

    def test_stop(self):
        assert self.ctx.is_stopped is False
        self.ctx.stop()
        assert self.ctx.is_stopped is True

    def test_is_reply_to_bot_detects_quoted_assistant_message(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.message_logs.insert(
            MessageLogRecord(
                session_id=self.session.id,
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
        ctx = MessageContext(
            event=self.event,
            message=Message.from_elements(
                MessageElement.quote("bot-msg-1"),
                MessageElement.text(" follow-up"),
            ),
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
            database=db,
        )
        assert ctx.is_reply_to_bot() is True

    def test_is_reply_to_bot_ignores_quoted_user_message(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.message_logs.insert(
            MessageLogRecord(
                session_id=self.session.id,
                platform_msg_id="user-msg-1",
                sender_id="user-2",
                sender_name="Other User",
                content_json="[]",
                raw_text="earlier user message",
                role="user",
                is_read=True,
                is_mentioned=False,
                created_at=time.time() * 1000,
            )
        )
        ctx = MessageContext(
            event=self.event,
            message=Message.from_elements(
                MessageElement.quote("user-msg-1"),
                MessageElement.text(" follow-up"),
            ),
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
            database=db,
        )
        assert ctx.is_reply_to_bot() is False


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

    @pytest.mark.asyncio
    async def test_pipeline_tracks_messages_in_context_manager(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        context_manager = ContextManager(db.message_logs)
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            context_manager=context_manager,
        )

        async def handler(ctx):
            await ctx.send("reply from bot")

        self.event_bus.on("message-created", handler)
        event = make_event("hello tracked")
        await pipeline.process_event(event, self.adapter)

        turns = context_manager.get_context_inputs("test-bot:private:user-1")["history_turns"]
        assert [turn["role"] for turn in turns] == ["user", "assistant"]
        assert turns[0]["content"] == "hello tracked"
        assert turns[1]["content"] == "reply from bot"

    @pytest.mark.asyncio
    async def test_pipeline_tracks_image_messages_in_context_manager(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        media_service = MediaService(db)
        context_manager = ContextManager(db.message_logs, media_service=media_service)
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            context_manager=context_manager,
            media_service=media_service,
        )

        image_path = tmp_path / "assets" / "tracked.png"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (8, 8), (0, 255, 0)).save(image_path)
        event = make_event(Message.from_elements(MessageElement.img(str(image_path))).to_xml())

        await pipeline.process_event(event, self.adapter)

        turns = context_manager.get_context_inputs("test-bot:private:user-1")["history_turns"]
        assert [turn["role"] for turn in turns] == ["user"]
        assert turns[0]["content"] == "[图片]"

    @pytest.mark.asyncio
    async def test_pipeline_updates_identity_store_from_user_messages(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        identity_store = IdentityStore(tmp_path / "identities.json")
        context_manager = ContextManager(db.message_logs, identity_store=identity_store)
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            context_manager=context_manager,
        )

        async def handler(_ctx):
            return None

        self.event_bus.on("message-created", handler)

        event = make_event("hello identity")
        event = event.model_copy(
            update={"platform": "qq", "user": User(id="user-1", name="咖啡猫😺")}
        )
        await pipeline.process_event(event, self.adapter)

        payload = json.loads(identity_store.file_path.read_text(encoding="utf-8"))
        assert payload["platform"] == "qq"
        entry = next(item for item in payload["users"] if item["user_id"] == "user-1")
        assert entry["name"] == "咖啡猫"

    @pytest.mark.asyncio
    async def test_pipeline_uses_group_member_nick_for_sender_name(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        identity_store = IdentityStore(tmp_path / "identities.json")
        context_manager = ContextManager(db.message_logs, identity_store=identity_store)
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            context_manager=context_manager,
        )

        event = make_event("hello group", channel_type=0)
        event = event.model_copy(
            update={
                "platform": "qq",
                "guild": Guild(id="group-1"),
                "user": User(id="user-1", name="用户昵称"),
                "member": Member(nick="群内昵称"),
            }
        )

        await pipeline.process_event(event, self.adapter)

        rows = db.message_logs.get_recent("test-bot:group:group-1:group:1", limit=1)
        assert rows[0]["sender_name"] == "群内昵称"
        turns = context_manager.get_context_inputs("test-bot:group:group-1:group:1")[
            "history_turns"
        ]
        assert turns[0]["sender_name"] == "群内昵称"
        payload = json.loads(identity_store.file_path.read_text(encoding="utf-8"))
        entry = next(item for item in payload["users"] if item["user_id"] == "user-1")
        assert entry["name"] == "群内昵称"

    @pytest.mark.asyncio
    async def test_pipeline_routes_private_messages_to_immediate_profile(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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

        await pipeline.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "immediate"

    @pytest.mark.asyncio
    async def test_pipeline_routes_group_messages_to_balanced_profile_by_default(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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
                        "response_profile": response_profile,
                        "is_mentioned": is_mentioned,
                        "attention_multiplier": attention_multiplier,
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

        await pipeline.process_event(make_event("hello group", channel_type=0), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "balanced"
        assert scheduler.calls[0]["is_mentioned"] is False

    @pytest.mark.asyncio
    async def test_pipeline_routes_priority_group_messages_to_immediate_profile(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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
                        "response_profile": response_profile,
                        "is_mentioned": is_mentioned,
                        "attention_multiplier": attention_multiplier,
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

        await pipeline.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "immediate"
        assert scheduler.calls[0]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_pipeline_attention_multiplier_for_poke_and_at_other(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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
                        "is_mentioned": is_mentioned,
                        "attention_multiplier": attention_multiplier,
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

        await pipeline.process_event(
            make_event('<sb:poke target="bot-1" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await pipeline.process_event(
            make_event('<sb:poke target="user-2" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await pipeline.process_event(
            make_event('<at id="user-2"/>hello', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert [call["attention_multiplier"] for call in scheduler.calls] == [2.0, 0.2, 0.6]
        assert [call["is_mentioned"] for call in scheduler.calls] == [False, False, False]

        rows = db.message_logs.get_recent("test-bot:group:group:1", limit=3)
        assert rows[0]["raw_text"] == "[戳一戳: 戳了你一下]"
        assert rows[1]["raw_text"] == "[戳一戳: 戳了用户 user-2 一下]"
        assert rows[2]["raw_text"] == "[@用户 user-2]hello"

    @pytest.mark.asyncio
    async def test_pipeline_uses_canonical_private_response_profile_config(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.bot_configs.upsert(
            BotConfigRecord(
                uuid="cfg-private-profile",
                instance_id=self.adapter.instance_id,
                config={"response_profile_private": "passive"},
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
                self.calls.append({"response_profile": response_profile})

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

        await pipeline.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "passive"

    @pytest.mark.asyncio
    async def test_pipeline_uses_canonical_group_and_priority_profiles(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.bot_configs.upsert(
            BotConfigRecord(
                uuid="cfg-group-profile",
                instance_id=self.adapter.instance_id,
                config={
                    "response_profile_group": "passive",
                    "response_profile_priority": "balanced",
                },
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
                        "response_profile": response_profile,
                        "is_mentioned": is_mentioned,
                        "attention_multiplier": attention_multiplier,
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

        await pipeline.process_event(make_event("hello group", channel_type=0), self.adapter)
        await pipeline.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 2
        assert scheduler.calls[0]["response_profile"] == "passive"
        assert scheduler.calls[1]["response_profile"] == "balanced"
        assert scheduler.calls[1]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_pipeline_attention_scheduler_skips_when_plugin_already_replied(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        class RecordingAttentionScheduler:
            def __init__(self) -> None:
                self.calls = 0

            async def on_message(self, *args, **kwargs) -> None:
                self.calls += 1

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

        async def handler(ctx):
            await ctx.send("plugin reply")

        self.event_bus.on("message-created", handler)
        await pipeline.process_event(make_event("hello plugin"), self.adapter)
        await asyncio.sleep(0)

        assert len(self.adapter.sent) == 1
        assert Message(elements=self.adapter.sent[0][1]).get_text() == "plugin reply"
        assert scheduler.calls == 0

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
