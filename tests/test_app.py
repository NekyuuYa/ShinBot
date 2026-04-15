"""Tests for application orchestrator."""

from __future__ import annotations

import sys
import types

import pytest

from shinbot.core.application.app import ShinBot
from shinbot.core.plugins.context import PluginContext
from shinbot.core.plugins.types import PluginState
from tests.conftest import MockAdapter, make_message_event


class TestShinBotInit:
    def test_all_subsystems_initialized(self):
        bot = ShinBot()
        assert bot.event_bus is not None
        assert bot.command_registry is not None
        assert bot.session_manager is not None
        assert bot.permission_engine is not None
        assert bot.tool_registry is not None
        assert bot.tool_manager is not None
        assert bot.adapter_manager is not None
        assert bot.plugin_manager is not None
        assert bot.pipeline is not None
        assert bot.model_runtime is not None

    def test_database_is_initialized_when_data_dir_is_provided(self, tmp_path):
        bot = ShinBot(data_dir=tmp_path)
        assert bot.database is not None
        assert (tmp_path / "db" / "shinbot.sqlite3").exists()

    def test_plugin_manager_shares_registry(self):
        """PluginManager must use the same registry as the pipeline."""
        bot = ShinBot()
        # Register a command via plugin and verify pipeline can resolve it
        ctx = PluginContext("test", bot.command_registry, bot.event_bus)

        @ctx.on_command("ping")
        async def handler(c, args):
            pass

        assert bot.command_registry.get("ping") is not None


class TestAddAdapter:
    def test_add_adapter_registers_instance(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        assert adapter is not None
        assert bot.adapter_manager.get_instance("inst1") is adapter

    def test_add_adapter_sets_event_callback(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        assert adapter._event_callback is not None

    def test_add_adapter_missing_platform_raises(self):
        bot = ShinBot()
        with pytest.raises(ValueError, match="No adapter registered"):
            bot.add_adapter("inst1", "unknown_platform")


class TestLoadPlugin:
    def setup_method(self):
        # Clean up test modules
        for k in list(sys.modules.keys()):
            if k.startswith("test_app_plugin_"):
                del sys.modules[k]

    def teardown_method(self):
        for k in list(sys.modules.keys()):
            if k.startswith("test_app_plugin_"):
                del sys.modules[k]

    def _register_module(self, name: str, setup_fn=None):
        mod = types.ModuleType(name)
        if setup_fn is not None:
            mod.setup = setup_fn
        sys.modules[name] = mod
        return mod

    def test_load_plugin_delegates_to_manager(self):
        bot = ShinBot()

        def setup(ctx: PluginContext):
            @ctx.on_command("greet")
            async def greet(c, args):
                pass

        self._register_module("test_app_plugin_greet", setup_fn=setup)
        meta = bot.load_plugin("greet", "test_app_plugin_greet")
        assert meta.state == PluginState.ACTIVE
        assert meta.name == "greet"

    @pytest.mark.asyncio
    async def test_load_plugin_async(self):
        bot = ShinBot()

        async def async_setup(ctx: PluginContext):
            @ctx.on_command("async_greet")
            async def h(c, args):
                pass

        self._register_module("test_app_plugin_async", setup_fn=async_setup)
        meta = await bot.load_plugin_async("ag", "test_app_plugin_async")
        assert meta.state == PluginState.ACTIVE


class TestOnEvent:
    @pytest.mark.asyncio
    async def test_on_event_routes_to_pipeline(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        events_processed = []

        async def interceptor(ctx):
            events_processed.append(ctx.event.type)
            return True

        bot.pipeline.add_interceptor(interceptor)

        event = make_message_event(content="hello", instance_id="inst1")
        await bot.on_event(event, adapter)
        assert "message-created" in events_processed

    @pytest.mark.asyncio
    async def test_on_event_handles_exceptions_gracefully(self):
        """on_event must not raise — it catches and logs all errors."""
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        async def exploding_interceptor(ctx):
            raise RuntimeError("boom")

        bot.pipeline.add_interceptor(exploding_interceptor)

        event = make_message_event(content="hello", instance_id="inst1")
        # Should not raise
        await bot.on_event(event, adapter)


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_starts_adapters(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        await bot.start()
        assert adapter.started is True

    @pytest.mark.asyncio
    async def test_shutdown_stops_adapters(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        await bot.start()
        await bot.shutdown()
        assert adapter.stopped is True
        assert len(bot.adapter_manager.all_instances) == 0


class TestEventCallback:
    @pytest.mark.asyncio
    async def test_adapter_callback_fires_pipeline(self):
        """Verify the event callback wired by add_adapter works end-to-end."""
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        received = []

        async def interceptor(ctx):
            received.append(ctx.event)
            return False  # block to avoid further processing

        bot.pipeline.add_interceptor(interceptor, priority=0)

        event = make_message_event(content="/ping", instance_id="inst1")

        # Call the wired callback directly
        await adapter._event_callback(event)

        assert len(received) == 1
        assert received[0].type == "message-created"
