"""Tests for platform adapter management."""

import pytest

from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.schema.elements import MessageElement

# ── Test adapter implementation ──────────────────────────────────────


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str, platform: str, **kwargs):
        super().__init__(instance_id, platform)
        self.started = False
        self.shut_down = False
        self.sent_messages: list[tuple[str, list[MessageElement]]] = []
        self.api_calls: list[tuple[str, dict]] = []

    async def start(self):
        self.started = True

    async def shutdown(self):
        self.shut_down = True

    async def send(self, target_session, elements):
        self.sent_messages.append((target_session, elements))
        return MessageHandle(message_id="msg-1", adapter_ref=self)

    async def call_api(self, method, params):
        self.api_calls.append((method, params))
        return {"ok": True}

    async def get_capabilities(self):
        return {
            "elements": ["text", "at", "img"],
            "actions": ["message.create"],
            "limits": {},
        }


class TestMessageHandle:
    def test_repr(self):
        h = MessageHandle("msg-123")
        assert "msg-123" in repr(h)

    @pytest.mark.asyncio
    async def test_edit_without_adapter_raises(self):
        h = MessageHandle("msg-1")
        with pytest.raises(RuntimeError):
            await h.edit([MessageElement.text("new")])

    @pytest.mark.asyncio
    async def test_recall_without_adapter_raises(self):
        h = MessageHandle("msg-1")
        with pytest.raises(RuntimeError):
            await h.recall()

    @pytest.mark.asyncio
    async def test_recall_after_send_calls_delete_api(self):
        adapter = MockAdapter("test-1", "mock")
        handle = await adapter.send("session-1", [MessageElement.text("hello")])

        await handle.recall()

        assert adapter.api_calls[-1] == ("message.delete", {"message_id": "msg-1"})


class TestBaseAdapter:
    def test_mock_adapter_creation(self):
        a = MockAdapter("test-1", "mock")
        assert a.instance_id == "test-1"
        assert a.platform == "mock"
        assert repr(a) == "<MockAdapter instance='test-1' platform='mock'>"


class TestAdapterManager:
    def setup_method(self):
        self.mgr = AdapterManager()
        self.mgr.register_adapter("mock", MockAdapter)

    def test_register_adapter(self):
        assert "mock" in self.mgr.registered_platforms

    def test_register_non_callable_raises(self):
        with pytest.raises(TypeError):
            self.mgr.register_adapter("bad", 42)  # type: ignore

    def test_register_callable_factory(self):
        def my_factory(instance_id, platform, **kwargs):
            return MockAdapter(instance_id, platform)

        self.mgr.register_adapter("factory_platform", my_factory)
        assert "factory_platform" in self.mgr.registered_platforms

    def test_create_instance(self):
        adapter = self.mgr.create_instance("bot-1", "mock")
        assert isinstance(adapter, MockAdapter)
        assert adapter.instance_id == "bot-1"

    def test_create_duplicate_instance_raises(self):
        self.mgr.create_instance("bot-1", "mock")
        with pytest.raises(ValueError, match="already exists"):
            self.mgr.create_instance("bot-1", "mock")

    def test_create_unknown_platform_raises(self):
        with pytest.raises(ValueError, match="No adapter registered"):
            self.mgr.create_instance("bot-1", "unknown")

    def test_get_instance(self):
        self.mgr.create_instance("bot-1", "mock")
        assert self.mgr.get_instance("bot-1") is not None
        assert self.mgr.get_instance("bot-99") is None

    def test_get_instances_by_platform(self):
        self.mgr.create_instance("bot-1", "mock")
        self.mgr.create_instance("bot-2", "mock")
        assert len(self.mgr.get_instances_by_platform("mock")) == 2
        assert len(self.mgr.get_instances_by_platform("other")) == 0

    def test_all_instances(self):
        self.mgr.create_instance("bot-1", "mock")
        assert len(self.mgr.all_instances) == 1

    def test_remove_instance(self):
        self.mgr.create_instance("bot-1", "mock")
        removed = self.mgr.remove_instance("bot-1")
        assert removed is not None
        assert self.mgr.get_instance("bot-1") is None

    @pytest.mark.asyncio
    async def test_start_all(self):
        a = self.mgr.create_instance("bot-1", "mock")
        await self.mgr.start_all()
        assert a.started  # type: ignore

    @pytest.mark.asyncio
    async def test_shutdown_all(self):
        a = self.mgr.create_instance("bot-1", "mock")
        await self.mgr.shutdown_all()
        assert a.shut_down  # type: ignore

    @pytest.mark.asyncio
    async def test_get_capabilities(self):
        self.mgr.create_instance("bot-1", "mock")
        caps = await self.mgr.get_capabilities("bot-1")
        assert caps is not None
        assert "text" in caps["elements"]

    def test_unregister_adapter(self):
        self.mgr.unregister_adapter("mock")
        assert "mock" not in self.mgr.registered_platforms

    def test_event_callback(self):
        called = []
        adapter = self.mgr.create_instance(
            "bot-1", "mock", event_callback=lambda e: called.append(e)
        )
        assert adapter._event_callback is not None
