"""Tests for platform adapter management."""

import asyncio

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

    def test_unregister_owner_restores_previous_factory(self):
        def original_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "original"
            return adapter

        def override_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "override"
            return adapter

        self.mgr.register_adapter("stacked", original_factory, owner="plugin-a")
        self.mgr.register_adapter("stacked", override_factory, owner="plugin-b")

        self.mgr.unregister_adapter("stacked", owner="plugin-b")

        adapter = self.mgr.create_instance("stacked-1", "stacked")
        assert adapter.factory_source == "original"  # type: ignore[attr-defined]

    def test_unregister_earlier_owner_keeps_later_override(self):
        def earlier_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "earlier"
            return adapter

        def later_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "later"
            return adapter

        self.mgr.register_adapter("stacked", earlier_factory, owner="plugin-a")
        self.mgr.register_adapter("stacked", later_factory, owner="plugin-b")

        self.mgr.unregister_adapter("stacked", owner="plugin-a")

        adapter = self.mgr.create_instance("stacked-1", "stacked")
        assert adapter.factory_source == "later"  # type: ignore[attr-defined]

    def test_unregister_without_owner_clears_owned_registration_stack(self):
        self.mgr.register_adapter("stacked", MockAdapter, owner="plugin-a")
        self.mgr.register_adapter("stacked", MockAdapter, owner="plugin-b")

        self.mgr.unregister_adapter("stacked")

        assert "stacked" not in self.mgr.registered_platforms

    @pytest.mark.asyncio
    async def test_suspend_resume_owner_rebuilds_running_instance_from_new_factory(self):
        callback_events: list[str] = []

        def callback(event: str):
            callback_events.append(event)

        def old_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "old"
            adapter.factory_kwargs = kwargs
            return adapter

        def new_factory(instance_id, platform, **kwargs):
            adapter = MockAdapter(instance_id, platform, **kwargs)
            adapter.factory_source = "new"
            adapter.factory_kwargs = kwargs
            return adapter

        self.mgr.register_adapter("owned", old_factory, owner="plugin-a")
        original = self.mgr.create_instance(
            "owned-1",
            "owned",
            event_callback=callback,
            token="secret",
        )
        await self.mgr.start_instance("owned-1")

        assert self.mgr.get_instance_owner("owned-1") == "plugin-a"
        assert await self.mgr.suspend_owner_instances("plugin-a") == ["owned-1"]
        assert original.shut_down  # type: ignore[attr-defined]
        assert self.mgr.get_instance("owned-1") is None
        assert self.mgr.has_instance_spec("owned-1")

        self.mgr.unregister_adapter("owned", owner="plugin-a")
        self.mgr.register_adapter("owned", new_factory, owner="plugin-a")
        assert await self.mgr.resume_owner_instances("plugin-a") == ["owned-1"]

        restored = self.mgr.get_instance("owned-1")
        assert restored is not None
        assert restored is not original
        assert restored.factory_source == "new"  # type: ignore[attr-defined]
        assert restored.factory_kwargs == {"token": "secret"}  # type: ignore[attr-defined]
        assert self.mgr.is_running("owned-1")

        assert original._event_callback is not None
        assert restored._event_callback is not None
        assert await original._event_callback("stale") is None
        assert callback_events == []
        assert await restored._event_callback("fresh") is None
        assert callback_events == ["fresh"]

        original._notify_connection_state(True)
        assert not self.mgr.is_connected("owned-1")
        restored._notify_connection_state(True)
        assert self.mgr.is_connected("owned-1")

    @pytest.mark.asyncio
    async def test_suspend_owner_ignores_instances_created_by_later_override(self):
        self.mgr.register_adapter("owned", MockAdapter, owner="plugin-a")
        self.mgr.register_adapter("owned", MockAdapter, owner="plugin-b")
        adapter = self.mgr.create_instance("owned-1", "owned")
        await self.mgr.start_instance("owned-1")

        assert self.mgr.get_instance_owner("owned-1") == "plugin-b"
        assert await self.mgr.suspend_owner_instances("plugin-a") == []
        assert self.mgr.get_instance("owned-1") is adapter
        assert self.mgr.is_running("owned-1")

    @pytest.mark.asyncio
    async def test_suspend_owner_serializes_with_in_flight_start(self):
        start_entered = asyncio.Event()
        release_start = asyncio.Event()

        class BlockingStartAdapter(MockAdapter):
            async def start(self):
                start_entered.set()
                await release_start.wait()
                await super().start()

        self.mgr.register_adapter("owned", BlockingStartAdapter, owner="plugin-a")
        adapter = self.mgr.create_instance("owned-1", "owned")
        start_task = asyncio.create_task(self.mgr.start_instance("owned-1"))
        await start_entered.wait()

        suspend_task = asyncio.create_task(self.mgr.suspend_owner_instances("plugin-a"))
        await asyncio.sleep(0)
        assert not suspend_task.done()
        assert self.mgr.get_instance("owned-1") is adapter

        release_start.set()
        await start_task
        assert await suspend_task == ["owned-1"]
        assert adapter.shut_down  # type: ignore[attr-defined]
        assert self.mgr.get_instance("owned-1") is None
        assert not self.mgr.is_running("owned-1")

    @pytest.mark.asyncio
    async def test_suspend_owner_detaches_all_instances_when_one_shutdown_fails(self):
        class FailingShutdownAdapter(MockAdapter):
            def __init__(self, instance_id: str, platform: str):
                super().__init__(instance_id, platform)
                self.shutdown_attempted = False
                self.allow_shutdown = False

            async def shutdown(self):
                self.shutdown_attempted = True
                if not self.allow_shutdown:
                    raise RuntimeError("shutdown failed")
                await super().shutdown()

        created: list[MockAdapter] = []

        def factory(instance_id, platform, **_kwargs):
            if instance_id == "failing":
                adapter = FailingShutdownAdapter(instance_id, platform)
            else:
                adapter = MockAdapter(instance_id, platform)
            created.append(adapter)
            return adapter

        self.mgr.register_adapter("owned", factory, owner="plugin-a")
        failing = self.mgr.create_instance("failing", "owned")
        healthy = self.mgr.create_instance("healthy", "owned")
        await self.mgr.start_instance("failing")
        await self.mgr.start_instance("healthy")

        assert await self.mgr.suspend_owner_instances("plugin-a") == [
            "failing",
            "healthy",
        ]

        assert failing.shutdown_attempted  # type: ignore[attr-defined]
        assert healthy.shut_down  # type: ignore[attr-defined]
        assert self.mgr.get_instance("failing") is None
        assert self.mgr.get_instance("healthy") is None
        assert self.mgr.has_instance_spec("failing")
        assert self.mgr.has_instance_spec("healthy")
        assert not self.mgr.is_running("failing")
        assert not self.mgr.is_running("healthy")

        with pytest.raises(RuntimeError, match="previous adapter is still active"):
            await self.mgr.resume_owner_instances("plugin-a")
        assert len(created) == 2
        assert self.mgr.get_instance("failing") is None
        assert self.mgr.get_instance("healthy") is None

        failing.allow_shutdown = True  # type: ignore[attr-defined]
        assert await self.mgr.resume_owner_instances("plugin-a") == [
            "failing",
            "healthy",
        ]
        assert len(created) == 4
        assert self.mgr.get_instance("failing") is not failing
        assert self.mgr.get_instance("healthy") is not healthy

    def test_event_callback(self):
        called = []
        adapter = self.mgr.create_instance(
            "bot-1", "mock", event_callback=lambda e: called.append(e)
        )
        assert adapter._event_callback is not None

    @pytest.mark.asyncio
    async def test_connection_state_reports_connected_and_disconnected(self):
        self.mgr.create_instance("bot-1", "mock")
        await self.mgr.start_instance("bot-1")

        self.mgr.mark_connected("bot-1", at=10.0)
        assert self.mgr.is_connected("bot-1") is True
        assert self.mgr.is_available("bot-1", now=10.0, offline_grace_seconds=0.0) is True

        self.mgr.mark_disconnected("bot-1", at=20.0)
        assert self.mgr.is_connected("bot-1") is False
        assert self.mgr.is_available("bot-1", now=25.0, offline_grace_seconds=10.0) is True
        assert self.mgr.is_available("bot-1", now=31.0, offline_grace_seconds=10.0) is False

    @pytest.mark.asyncio
    async def test_running_instance_without_explicit_connection_state_is_available(self):
        self.mgr.create_instance("bot-1", "mock")
        await self.mgr.start_instance("bot-1")

        assert self.mgr.is_connected("bot-1") is False
        assert self.mgr.is_available("bot-1", now=10.0) is True

    @pytest.mark.asyncio
    async def test_connection_state_is_unavailable_when_not_running(self):
        self.mgr.create_instance("bot-1", "mock")
        self.mgr.mark_connected("bot-1", at=10.0)

        assert self.mgr.is_connected("bot-1") is False
        assert self.mgr.is_available("bot-1", now=10.0) is False
