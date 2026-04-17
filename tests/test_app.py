"""Tests for application orchestrator."""

from __future__ import annotations

import sys
import types
from uuid import uuid4

import pytest

from shinbot.core.application.app import ShinBot
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.types import PluginState
from shinbot.persistence import (
    AgentRecord,
    BotConfigRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)
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

    def test_load_plugin_delegates_to_manager(self):
        bot = ShinBot()

        def setup(plg: Plugin):
            @plg.on_command("greet")
            async def greet(c, args):
                pass

        self._register_module("test_app_plugin_greet", setup_fn=setup)
        meta = bot.load_plugin("greet", "test_app_plugin_greet")
        assert meta.state == PluginState.ACTIVE
        assert meta.name == "greet"

    @pytest.mark.asyncio
    async def test_load_plugin_async(self):
        bot = ShinBot()

        async def async_setup(plg: Plugin):
            @plg.on_command("async_greet")
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

    @pytest.mark.asyncio
    async def test_on_event_invokes_default_agent_runtime(self, tmp_path, monkeypatch):
        bot = ShinBot(data_dir=tmp_path)
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("inst1", "mock")

        persona_prompt_uuid = str(uuid4())
        persona_uuid = str(uuid4())
        agent_uuid = str(uuid4())
        bot.database.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid=persona_prompt_uuid,
                prompt_id=f"persona.{persona_uuid}",
                name="Persona Prompt",
                source_type="persona",
                source_id=persona_uuid,
                stage="identity",
                type="static_text",
                content="You are a concise assistant.",
            )
        )
        bot.database.personas.upsert(
            PersonaRecord(
                uuid=persona_uuid,
                name="Assistant",
                prompt_definition_uuid=persona_prompt_uuid,
            )
        )
        bot.database.agents.upsert(
            AgentRecord(
                uuid=agent_uuid,
                agent_id="agent.default",
                name="Default Agent",
                persona_uuid=persona_uuid,
                config={"modelId": "openai-main/gpt-fast"},
            )
        )
        bot.database.bot_configs.upsert(
            BotConfigRecord(
                uuid=str(uuid4()),
                instance_id="inst1",
                default_agent_uuid=agent_uuid,
            )
        )
        bot.database.model_registry.upsert_provider(
            ModelProviderRecord(
                id="openai-main",
                type="openai",
                display_name="OpenAI Main",
            )
        )
        bot.database.model_registry.upsert_model(
            ModelDefinitionRecord(
                id="openai-main/gpt-fast",
                provider_id="openai-main",
                litellm_model="openai/gpt-4.1-mini",
                display_name="GPT Fast",
                capabilities=["chat"],
            )
        )

        captured: dict[str, object] = {}

        async def fake_generate(call):
            captured["messages"] = list(call.messages)
            captured["model_id"] = call.model_id
            return type("FakeResult", (), {"text": "agent reply", "execution_id": "exec-1"})()

        monkeypatch.setattr(bot.model_runtime, "generate", fake_generate)

        event = make_message_event(content="hello from user", instance_id="inst1")
        await bot.on_event(event, adapter)

        assert len(adapter.sent) == 1
        assert adapter.sent[0][1][0].attrs["content"] == "agent reply"
        assert captured["model_id"] == "openai-main/gpt-fast"
        messages = captured["messages"]
        assert isinstance(messages, list)
        assert messages[0]["role"] == "system"
        system_text = " ".join(block["text"] for block in messages[0]["content"])
        assert "ShinBot" in system_text
        assert "concise assistant" in system_text
        assert any(message.get("content") == "hello from user" for message in messages[1:])


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
