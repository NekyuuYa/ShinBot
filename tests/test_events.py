"""Tests for shinbot.models.events — UnifiedEvent and related models."""

import json

import pytest

from shinbot.models.events import (
    Channel,
    Guild,
    Login,
    Member,
    MessagePayload,
    UnifiedEvent,
    User,
)


class TestUser:
    def test_basic(self):
        u = User(id="123", name="Alice")
        assert u.id == "123"
        assert u.name == "Alice"
        assert u.is_bot is False

    def test_bot_user(self):
        u = User(id="1", is_bot=True)
        assert u.is_bot is True

    def test_minimal(self):
        u = User(id="1")
        assert u.name is None
        assert u.avatar is None


class TestChannel:
    def test_basic(self):
        ch = Channel(id="ch-1", name="general", type=0)
        assert ch.id == "ch-1"
        assert ch.type == 0

    def test_private(self):
        ch = Channel(id="private:123", type=1)
        assert ch.type == 1

    def test_default_type(self):
        ch = Channel(id="ch-1")
        assert ch.type == 0


class TestGuild:
    def test_basic(self):
        g = Guild(id="guild-1", name="MyGuild")
        assert g.id == "guild-1"
        assert g.name == "MyGuild"


class TestLogin:
    def test_full(self):
        login = Login(
            sn=1,
            user=User(id="bot-1", name="yui"),
            adapter="llonebot",
            platform="llonebot",
            status=1,
            features=["message.create", "message.delete"],
        )
        assert login.platform == "llonebot"
        assert len(login.features) == 2

    def test_minimal(self):
        login = Login()
        assert login.sn is None
        assert login.features == []


class TestMessagePayload:
    def test_basic(self):
        mp = MessagePayload(id="msg-1", content="hello world", created_at=1700000000)
        assert mp.id == "msg-1"
        assert mp.content == "hello world"

    def test_xml_content(self):
        mp = MessagePayload(id="msg-2", content='hi <at id="123"/>')
        assert "<at" in mp.content


class TestMember:
    def test_basic(self):
        m = Member(nick="Alice", roles=["admin", "mod"])
        assert m.nick == "Alice"
        assert len(m.roles) == 2

    def test_minimal(self):
        m = Member()
        assert m.nick is None
        assert m.roles == []


class TestUnifiedEvent:
    def test_message_event(self):
        event = UnifiedEvent(
            id=1,
            type="message-created",
            self_id="bot-1",
            platform="llonebot",
            timestamp=1700000000,
            user=User(id="user-1", name="Alice"),
            channel=Channel(id="ch-1", type=0),
            message=MessagePayload(id="msg-1", content="hello"),
        )
        assert event.is_message_event is True
        assert event.sender_id == "user-1"
        assert event.channel_id == "ch-1"
        assert event.message_content == "hello"
        assert event.is_private is False

    def test_private_message(self):
        event = UnifiedEvent(
            type="message-created",
            channel=Channel(id="private:123", type=1),
        )
        assert event.is_private is True

    def test_non_message_event(self):
        event = UnifiedEvent(type="member-joined")
        assert event.is_message_event is False

    def test_guild_id(self):
        event = UnifiedEvent(
            type="message-created",
            guild=Guild(id="guild-1"),
        )
        assert event.guild_id == "guild-1"

    def test_null_accessors(self):
        event = UnifiedEvent(type="some-event")
        assert event.sender_id is None
        assert event.channel_id is None
        assert event.guild_id is None
        assert event.message_content == ""

    def test_from_real_satori_json(self):
        """Parse a real Satori WebSocket event body."""
        raw = {
            "id": 1,
            "sn": 1,
            "type": "message-created",
            "self_id": "3649342015",
            "platform": "llonebot",
            "timestamp": 1775938758715,
            "login": {
                "sn": 1,
                "user": {
                    "id": "3649342015",
                    "name": "yui",
                    "avatar": "http://q.qlogo.cn/headimg_dl?dst_uin=3649342015&spec=640",
                    "is_bot": False,
                },
                "platform": "llonebot",
            },
            "message": {
                "id": "7627598887189479708",
                "content": "噼里啪啦噼里啪啦",
                "created_at": 1775938758000,
            },
            "user": {
                "id": "1917419834",
                "name": "Ginkoro",
                "avatar": "http://q.qlogo.cn/headimg_dl?dst_uin=1917419834&spec=640",
                "is_bot": False,
            },
            "channel": {"id": "private:1917419834", "name": "Ginkoro", "type": 1},
        }
        event = UnifiedEvent.model_validate(raw)
        assert event.type == "message-created"
        assert event.platform == "llonebot"
        assert event.sender_id == "1917419834"
        assert event.is_private is True
        assert event.message_content == "噼里啪啦噼里啪啦"
        assert event.login is not None
        assert event.login.user is not None
        assert event.login.user.name == "yui"

    def test_from_real_satori_with_image(self):
        """Parse a Satori event with mixed text + img content."""
        raw = {
            "id": 4,
            "type": "message-created",
            "self_id": "3649342015",
            "platform": "llonebot",
            "message": {
                "id": "msg-4",
                "content": '色情图片<img width="878" height="1920" src="https://example.com/img.png"/>',
            },
            "user": {"id": "user-1"},
            "channel": {"id": "private:user-1", "type": 1},
        }
        event = UnifiedEvent.model_validate(raw)
        assert event.is_message_event
        assert "<img" in event.message_content

    def test_message_event_types(self):
        for t in ("message-created", "message-updated", "message-deleted"):
            assert UnifiedEvent(type=t).is_message_event is True
        for t in ("member-joined", "guild-created", "reaction-added"):
            assert UnifiedEvent(type=t).is_message_event is False
