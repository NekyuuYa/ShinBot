"""Message ingress context-manager integration tests."""

import asyncio
import json

import pytest
from PIL import Image

from shinbot.agent.context import ContextManager
from shinbot.agent.identity import IdentityStore
from shinbot.agent.media import MediaService
from shinbot.core.dispatch.ingress import MessageIngress, RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteRule, RouteTable
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, Member, User

pytestmark = [pytest.mark.integration, pytest.mark.slow]


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


def make_event(content="hello", user_id="user-1", channel_type=1):
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id),
        channel=Channel(
            id=f"private:{user_id}" if channel_type == 1 else "group:1",
            type=channel_type,
        ),
        message=MessagePayload(id="msg-1", content=content),
    )


def add_message_route(table: RouteTable, *, target: str = "recorder") -> RouteRule:
    rule = RouteRule(
        id=f"route.{target}",
        priority=10,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=target,
    )
    table.register(rule)
    return rule


def make_ingress(
    db: DatabaseManager,
    *,
    context_manager: ContextManager,
    route_table: RouteTable | None = None,
    route_targets: RouteTargetRegistry | None = None,
    media_service: MediaService | None = None,
) -> MessageIngress:
    return MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=route_table if route_table is not None else RouteTable(),
        route_targets=route_targets,
        database=db,
        context_manager=context_manager,
        media_service=media_service,
    )


class TestMessageIngressContext:
    def setup_method(self):
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_ingress_tracks_messages_in_context_manager(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        context_manager = ContextManager(db.message_logs)
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()

        async def handler(context, _rule):
            message_context = context.require_message_context()
            await message_context.send("reply from bot")
            message_context.mark_trigger_read()

        targets.register("recorder", handler)
        ingress = make_ingress(
            db,
            context_manager=context_manager,
            route_table=table,
            route_targets=targets,
        )

        await ingress.process_event(make_event("hello tracked"), self.adapter)
        await asyncio.sleep(0)

        turns = context_manager.get_context_inputs("test-bot:private:user-1")["history_turns"]
        assert [turn["role"] for turn in turns] == ["user", "assistant"]
        assert turns[0]["content"] == "hello tracked"
        assert turns[1]["content"] == "reply from bot"

    @pytest.mark.asyncio
    async def test_ingress_tracks_image_messages_in_context_manager(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        media_service = MediaService(db)
        context_manager = ContextManager(db.message_logs, media_service=media_service)
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        targets.register(
            "recorder",
            lambda context, _rule: context.require_message_context().mark_trigger_read(),
        )
        ingress = make_ingress(
            db,
            context_manager=context_manager,
            route_table=table,
            route_targets=targets,
            media_service=media_service,
        )

        image_path = tmp_path / "assets" / "tracked.png"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (8, 8), (0, 255, 0)).save(image_path)
        event = make_event(Message.from_elements(MessageElement.img(str(image_path))).to_xml())

        await ingress.process_event(event, self.adapter)

        turns = context_manager.get_context_inputs("test-bot:private:user-1")["history_turns"]
        assert [turn["role"] for turn in turns] == ["user"]
        assert turns[0]["content"] == "[图片]"

    @pytest.mark.asyncio
    async def test_ingress_updates_identity_store_from_user_messages(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        identity_store = IdentityStore(tmp_path / "identities.json")
        context_manager = ContextManager(db.message_logs, identity_store=identity_store)
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        targets.register(
            "recorder",
            lambda context, _rule: context.require_message_context().mark_trigger_read(),
        )
        ingress = make_ingress(
            db,
            context_manager=context_manager,
            route_table=table,
            route_targets=targets,
        )

        event = make_event("hello identity")
        event = event.model_copy(
            update={"platform": "qq", "user": User(id="user-1", name="咖啡猫😺")}
        )
        await ingress.process_event(event, self.adapter)

        payload = json.loads(identity_store.file_path.read_text(encoding="utf-8"))
        assert payload["platform"] == "qq"
        entry = next(item for item in payload["users"] if item["user_id"] == "user-1")
        assert entry["name"] == "咖啡猫"

    @pytest.mark.asyncio
    async def test_ingress_uses_group_member_nick_for_sender_name(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        identity_store = IdentityStore(tmp_path / "identities.json")
        context_manager = ContextManager(db.message_logs, identity_store=identity_store)
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        targets.register(
            "recorder",
            lambda context, _rule: context.require_message_context().mark_trigger_read(),
        )
        ingress = make_ingress(
            db,
            context_manager=context_manager,
            route_table=table,
            route_targets=targets,
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

        await ingress.process_event(event, self.adapter)

        rows = db.message_logs.get_recent("test-bot:group:group-1:group:1", limit=1)
        assert rows[0]["sender_name"] == "群内昵称"
        turns = context_manager.get_context_inputs("test-bot:group:group-1:group:1")[
            "history_turns"
        ]
        assert turns[0]["sender_name"] == "群内昵称"
        payload = json.loads(identity_store.file_path.read_text(encoding="utf-8"))
        entry = next(item for item in payload["users"] if item["user_id"] == "user-1")
        assert entry["name"] == "群内昵称"
