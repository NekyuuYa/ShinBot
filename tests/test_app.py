"""Tests for application orchestrator."""

from __future__ import annotations

import asyncio
import sys
import types

import pytest

from shinbot.agent.attention.engine import AttentionConfig
from shinbot.agent.runtime import install_agent_runtime
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.core.dispatch.routing import RouteCondition
from shinbot.core.message_routes.command import CommandDef
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.types import PluginState
from shinbot.core.runtime import install_model_runtime
from shinbot.schema.events import UnifiedEvent
from shinbot.schema.resources import Channel, User
from tests.conftest import MockAdapter, make_message_event


class TestShinBotInit:
    def test_all_subsystems_initialized(self):
        bot = ShinBot()
        assert bot.event_bus is not None
        assert bot.command_registry is not None
        assert bot.keyword_registry is not None
        assert bot.session_manager is not None
        assert bot.permission_engine is not None
        assert bot.adapter_manager is not None
        assert bot.plugin_manager is not None
        assert bot.route_table is not None
        assert bot.route_targets is not None
        assert bot.message_ingress is not None
        assert bot.model_runtime is None
        assert bot.agent_runtime is None
        assert not hasattr(bot, "prompt_registry")
        assert not hasattr(bot, "tool_registry")
        assert not hasattr(bot, "attention_scheduler")

    def test_model_runtime_can_be_mounted_without_agent(self):
        bot = ShinBot()
        install_model_runtime(bot)
        assert bot.model_runtime is not None
        assert bot.agent_runtime is None

    def test_agent_runtime_can_be_mounted(self):
        bot = ShinBot()
        install_agent_runtime(bot)
        assert bot.agent_runtime is not None
        assert bot.agent_runtime.tool_registry is not None
        assert bot.agent_runtime.tool_manager is not None
        assert bot.model_runtime is not None

    def test_agent_runtime_reuses_mounted_model_runtime(self):
        bot = ShinBot()
        model_runtime = install_model_runtime(bot)
        install_agent_runtime(bot)
        assert bot.model_runtime is model_runtime
        assert bot.agent_runtime.model_runtime is model_runtime

    def test_database_is_initialized_when_data_dir_is_provided(self, tmp_path):
        bot = ShinBot(data_dir=tmp_path)
        assert bot.database is not None
        assert (tmp_path / "db" / "shinbot.sqlite3").exists()

    def test_attention_debug_parameter(self):
        bot_no_debug = ShinBot()
        install_agent_runtime(bot_no_debug, attention_debug=False)
        assert bot_no_debug.agent_runtime.attention_config.debug is False

        bot_debug = ShinBot()
        install_agent_runtime(bot_debug, attention_debug=True)
        assert bot_debug.agent_runtime.attention_config.debug is True

    def test_attention_config_parameter(self):
        config = AttentionConfig(decay_k=0.002, decay_idle_grace_seconds=300.0)
        bot = ShinBot()
        install_agent_runtime(bot, attention_config=config)
        assert bot.agent_runtime.attention_config.decay_k == 0.002
        assert bot.agent_runtime.attention_config.decay_idle_grace_seconds == 300.0

    def test_plugin_manager_shares_registry(self):
        """PluginManager must use the same registry as message ingress."""
        bot = ShinBot()
        # Register a command via plugin and verify ingress can resolve it.
        plg = Plugin("test", bot.command_registry, bot.event_bus)

        @plg.on_command("ping")
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

    def test_load_plugin_delegates_to_manager(self, tmp_path):
        bot = ShinBot(data_dir=tmp_path)

        def setup(plg: Plugin):
            @plg.on_command("greet")
            async def greet(c, args):
                pass

        self._register_module("test_app_plugin_greet", setup_fn=setup)
        meta = bot.load_plugin("greet", "test_app_plugin_greet")
        assert meta.state == PluginState.ACTIVE
        assert meta.name == "greet"

    @pytest.mark.asyncio
    async def test_load_plugin_async(self, tmp_path):
        bot = ShinBot(data_dir=tmp_path)

        async def async_setup(plg: Plugin):
            @plg.on_command("async_greet")
            async def h(c, args):
                pass

        self._register_module("test_app_plugin_async", setup_fn=async_setup)
        meta = await bot.load_plugin_async("ag", "test_app_plugin_async")
        assert meta.state == PluginState.ACTIVE


class TestOnEvent:
    @pytest.mark.asyncio
    async def test_on_event_routes_to_message_ingress(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        events_processed = []

        async def interceptor(ctx):
            events_processed.append(ctx.event.type)
            return True

        bot.message_ingress.add_interceptor(interceptor)

        event = make_message_event(content="hello", instance_id="inst1")
        await bot.on_event(event, adapter)
        assert "message-created" in events_processed

    @pytest.mark.asyncio
    async def test_on_event_executes_command_via_message_ingress(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        calls = []

        async def ping(ctx, args):
            calls.append((ctx.session_id, args))

        bot.command_registry.register(CommandDef(name="ping", handler=ping))

        event = make_message_event(content="/ping ok", instance_id="inst1")
        await bot.on_event(event, adapter)
        await asyncio.sleep(0)

        assert calls == [("inst1:group:ch-1", "ok")]

    @pytest.mark.asyncio
    async def test_on_event_routes_notice_to_event_bus(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        calls = []

        async def handler(event):
            calls.append(event.type)

        bot.event_bus.on("guild-member-added", handler)

        event = UnifiedEvent(
            type="guild-member-added",
            platform="mock",
            self_id="inst1",
            user=User(id="user-1"),
            channel=Channel(id="ch-1", type=0),
        )
        await bot.on_event(event, adapter)
        await asyncio.sleep(0)

        assert calls == ["guild-member-added"]

    @pytest.mark.asyncio
    async def test_on_event_executes_keyword_via_message_ingress(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        calls = []

        def setup(plg: Plugin):
            @plg.on_keyword("needle")
            async def keyword(ctx, match):
                calls.append((ctx.session_id, match.matched_text))

        plg = Plugin(
            "keyword-test",
            bot.command_registry,
            bot.event_bus,
            keyword_registry=bot.keyword_registry,
        )
        setup(plg)

        event = make_message_event(content="find needle", instance_id="inst1")
        await bot.on_event(event, adapter)
        await asyncio.sleep(0)

        assert calls == [("inst1:group:ch-1", "needle")]

    @pytest.mark.asyncio
    async def test_on_event_executes_plugin_route_via_message_ingress(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        calls = []

        def setup(plg: Plugin):
            @plg.on_route(
                RouteCondition(
                    event_types=frozenset({"message-created"}),
                    custom_matcher=lambda _event, message: message.text == "route me",
                ),
                rule_id="app-route",
                target="app-route-target",
            )
            async def app_route(ctx, rule):
                calls.append((ctx.require_message_context().session_id, rule.id))

        module_name = "test_app_plugin_route"
        mod = types.ModuleType(module_name)
        mod.setup = setup
        sys.modules[module_name] = mod
        try:
            meta = await bot.load_plugin_async("route-plugin", module_name)
            assert meta.routes == ["app-route"]

            event = make_message_event(content="route me", instance_id="inst1")
            await bot.on_event(event, adapter)
            await asyncio.sleep(0)

            assert calls == [("inst1:group:ch-1", "app-route")]
        finally:
            sys.modules.pop(module_name, None)

    @pytest.mark.asyncio
    async def test_on_event_emits_agent_entry_signal_via_registered_handler(self):
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")
        signals: list[AgentEntrySignal] = []

        async def handler(signal: AgentEntrySignal) -> None:
            signals.append(signal)

        bot.set_agent_entry_handler(handler)

        event = make_message_event(content="hello agent", instance_id="inst1")
        await bot.on_event(event, adapter)
        await asyncio.sleep(0)

        assert len(signals) == 1
        assert signals[0].session_id == "inst1:group:ch-1"
        assert signals[0].event_type == "message-created"
        assert signals[0].instance_id == "inst1"
        assert signals[0].platform == "mock"
        assert not hasattr(signals[0], "message")

    @pytest.mark.asyncio
    async def test_on_event_handles_exceptions_gracefully(self):
        """on_event must not raise — it catches and logs all errors."""
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        async def exploding_interceptor(ctx):
            raise RuntimeError("boom")

        bot.message_ingress.add_interceptor(exploding_interceptor)

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
    async def test_adapter_callback_fires_message_ingress(self):
        """Verify the event callback wired by add_adapter works end-to-end."""
        bot = ShinBot()
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        received = []

        async def interceptor(ctx):
            received.append(ctx.event)
            return False  # block to avoid further processing

        bot.message_ingress.add_interceptor(interceptor, priority=0)

        event = make_message_event(content="/ping", instance_id="inst1")

        # Call the wired callback directly
        await adapter._event_callback(event)

        assert len(received) == 1
        assert received[0].type == "message-created"
