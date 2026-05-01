"""Tests for message pipeline dispatch."""

import time

import pytest

from shinbot.core.dispatch.pipeline import MessageContext
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.state.session import Session
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    MessageLogRecord,
)
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, User

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

    def test_is_mentioned_detects_nested_at(self):
        ctx = MessageContext(
            event=self.event,
            message=Message.from_elements(
                MessageElement.quote(
                    "quoted-msg",
                    children=[MessageElement.at(id=" bot-1 ")],
                )
            ),
            session=self.session,
            adapter=self.adapter,
            permissions=set(),
        )
        assert ctx.is_mentioned is True

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

