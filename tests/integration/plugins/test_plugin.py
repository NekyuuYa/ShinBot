"""Tests for plugin lifecycle and registration."""

import asyncio
import inspect
import json
import sys
import tempfile
import types
from pathlib import Path

import pytest

import shinbot.core.plugins.dependencies as plugin_dependencies
from shinbot.agent.services.tools import ToolRegistry
from shinbot.builtin_plugins import shinbot_plugin_sleepy
from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootController
from shinbot.core.config_provider import ConfigProviderRegistry, load_provider_schema
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteTable
from shinbot.core.message_routes.command import CommandDef, CommandRegistry
from shinbot.core.message_routes.keyword import KeywordRegistry
from shinbot.core.platform.adapter_manager import AdapterManager
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.manager import PluginManager, _topo_sort
from shinbot.core.plugins.types import PluginRole, PluginState
from tests.conftest import MockAdapter, make_message_event


def _make_plugin_module(
    name: str = "test_plugin_mod",
    setup_fn=None,
    teardown_fn=None,
    **attrs,
):
    """Create a mock plugin module and register it in sys.modules."""
    mod = types.ModuleType(name)
    if setup_fn is not None:
        mod.setup = setup_fn  # type: ignore
    for k, v in attrs.items():
        setattr(mod, k, v)
    if teardown_fn is not None:
        mod.teardown = teardown_fn  # type: ignore
    sys.modules[name] = mod
    return mod


def _write_metadata_plugin(
    root: Path,
    *,
    plugin_id: str = "demo_plugin",
    module_body: str = "def setup(plg):\n    pass\n",
    schema_body: str | None = None,
) -> Path:
    plugins_dir = root / f"plugins_{plugin_id}"
    plugin_dir = plugins_dir / plugin_id
    plugin_dir.mkdir(parents=True)
    (plugins_dir / "__init__.py").write_text("", encoding="utf-8")
    (plugin_dir / "__init__.py").write_text(module_body, encoding="utf-8")
    (plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": plugin_id,
                "name": plugin_id,
                "version": "1.0.0",
                "entry": "__init__.py",
                "role": "logic",
            }
        ),
        encoding="utf-8",
    )
    if schema_body is not None:
        (plugin_dir / "config.schema.toml").write_text(schema_body, encoding="utf-8")
        (plugin_dir / "config.example.toml").write_text('api_key = ""\n', encoding="utf-8")
    return plugins_dir


def _repo_path(*parts: str) -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent.joinpath(*parts)
    raise RuntimeError("Could not locate repository root")


class TestPlugin:
    def setup_method(self):
        self.cmd_reg = CommandRegistry()
        self.event_bus = EventBus()
        self.keyword_registry = KeywordRegistry()
        self.route_table = RouteTable()
        self.route_targets = RouteTargetRegistry()
        self.tool_registry = ToolRegistry()
        self.plg = Plugin(
            "test-plugin",
            self.cmd_reg,
            self.event_bus,
            keyword_registry=self.keyword_registry,
            route_table=self.route_table,
            route_targets=self.route_targets,
            tool_registry=self.tool_registry,
        )

    def test_on_command_decorator(self):
        @self.plg.on_command("hello", aliases=["hi"], permission="cmd.hello")
        async def handler(ctx, args):
            pass

        assert self.cmd_reg.get("hello") is not None
        assert self.cmd_reg.get("hi") is not None
        assert "hello" in self.plg._registered_commands

    def test_on_event_decorator(self):
        @self.plg.on_event("guild-member-added")
        async def handler(event):
            pass

        assert self.event_bus.handler_count("guild-member-added") == 1
        assert "guild-member-added" in self.plg._registered_events

    def test_on_event_rejects_message_events(self):
        with pytest.raises(ValueError, match="RouteTable"):
            self.plg.on_event("message-created")

        assert self.event_bus.handler_count("message-created") == 0

    @pytest.mark.asyncio
    async def test_begin_deactivation_rejects_and_closes_new_task_coroutine(self):
        async def worker() -> None:
            await asyncio.sleep(0)

        coroutine = worker()
        self.plg.begin_deactivation()

        with pytest.raises(RuntimeError, match="during deactivation"):
            self.plg.create_task(coroutine, name="late-worker")

        assert self.plg.task_creation_frozen is True
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert self.plg.background_tasks == frozenset()

    @pytest.mark.asyncio
    async def test_cancel_background_tasks_blocks_derivative_task_creation(self):
        cancellation_started = asyncio.Event()
        derivative_rejected = asyncio.Event()

        async def derivative() -> None:
            await asyncio.Event().wait()

        async def worker() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                cancellation_started.set()
                try:
                    self.plg.create_task(derivative(), name="derivative")
                except RuntimeError:
                    derivative_rejected.set()

        task = self.plg.create_task(worker(), name="worker")
        await asyncio.sleep(0)

        await self.plg.cancel_background_tasks()

        assert task.cancelled()
        assert cancellation_started.is_set()
        assert derivative_rejected.is_set()
        assert self.plg.task_creation_frozen is True
        assert self.plg.background_tasks == frozenset()

    def test_adapter_factory_registration_is_owned_by_plugin(self):
        adapter_manager = AdapterManager()

        def original_factory(**kwargs):
            return kwargs

        def plugin_factory(**kwargs):
            return kwargs

        adapter_manager.register_adapter("shared", original_factory, owner="original-owner")
        plugin = Plugin(
            "adapter-plugin",
            self.cmd_reg,
            self.event_bus,
            adapter_manager=adapter_manager,
        )

        plugin.register_adapter_factory("shared", plugin_factory)
        adapter_manager.unregister_adapter("shared", owner=plugin.plugin_id)

        assert adapter_manager._factories["shared"] is original_factory
        assert plugin._registered_adapter_factories == ["shared"]

    def test_on_keyword_decorator(self):
        @self.plg.on_keyword("hello")
        async def handler(ctx, match):
            pass

        assert len(self.keyword_registry.all_keywords) == 1
        assert self.keyword_registry.all_keywords[0].pattern == "hello"
        assert self.keyword_registry.all_keywords[0].owner == "test-plugin"
        assert "hello" in self.plg._registered_keywords

    def test_on_route_decorator(self):
        @self.plg.on_route(
            RouteCondition(event_types=frozenset({"message-created"})),
            rule_id="custom-route",
            target="custom-target",
        )
        async def handler(ctx, rule):
            pass

        rules = self.route_table.rules
        assert len(rules) == 1
        assert rules[0].id == "custom-route"
        assert rules[0].target == "custom-target"
        assert rules[0].owner == "test-plugin"
        assert self.route_targets.get("custom-target") is not None
        assert self.plg._registered_routes == ["custom-route"]

    def test_on_route_rolls_back_target_when_rule_registration_fails(self):
        @self.plg.on_route(RouteCondition(), rule_id="duplicate-route", target="first-target")
        async def first(ctx, rule):
            pass

        with pytest.raises(ValueError, match="already registered"):
            self.plg.on_route(
                RouteCondition(),
                rule_id="duplicate-route",
                target="second-target",
            )(lambda ctx, rule: None)

        assert self.route_targets.get("first-target") is not None
        assert self.route_targets.get("second-target") is None

    def test_tool_decorator_registers_tool(self):
        @self.plg.tool(
            name="weather_query",
            description="query weather",
            permission="tools.weather.query",
            input_schema={
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        )
        async def weather_query(args, runtime):
            return {"city": args["city"]}

        definition = self.tool_registry.get_tool_by_name("weather_query")
        assert definition is not None
        assert definition.owner_id == "test-plugin"
        assert definition.permission == "tools.weather.query"


def test_plugin_manager_registers_provider_schema_from_plugin_module(tmp_path: Path) -> None:
    schema = """
[provider]
kind = "plugin"
id = "demo_plugin"
display_name = "Demo Plugin"

[[fields]]
path = "api_key"
type = "string"
default = ""
secret = true
""".strip()
    plugins_dir = _write_metadata_plugin(tmp_path, schema_body=schema + "\n")
    registry = ConfigProviderRegistry()
    mgr = PluginManager(CommandRegistry(), EventBus(), data_dir=tmp_path, config_provider_registry=registry)

    loaded = mgr.load_plugins_from_metadata_dir(plugins_dir)

    assert [plugin.id for plugin in loaded] == ["demo_plugin"]
    provider = registry.get("plugin", "demo_plugin")
    assert provider is not None
    assert provider.display_name == "Demo Plugin"
    assert provider.example_toml == 'api_key = ""\n'
    assert registry.default_config("plugin", "demo_plugin") == {"api_key": ""}


def test_plugin_manager_ignores_plugins_without_provider_schema(tmp_path: Path) -> None:
    plugins_dir = _write_metadata_plugin(tmp_path, plugin_id="no_schema_plugin")
    registry = ConfigProviderRegistry()
    mgr = PluginManager(CommandRegistry(), EventBus(), data_dir=tmp_path, config_provider_registry=registry)

    loaded = mgr.load_plugins_from_metadata_dir(plugins_dir)

    assert [plugin.id for plugin in loaded] == ["no_schema_plugin"]
    assert registry.catalog() == []


def test_sleepy_plugin_applies_runtime_threshold_delta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Runtime:
        def __init__(self) -> None:
            self.calls: list[tuple[float, str]] = []

        def set_active_chat_threshold_delta(self, delta: float, *, source: str = "") -> None:
            self.calls.append((delta, source))

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[[plugins]]
id = "shinbot_plugin_sleepy"

[plugins.config]
enabled = true

[[plugins.config.schedules]]
name = "Always"
start_time = "00:00"
end_time = "23:59"
threshold_delta = 4.5
enabled = true
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv", ["shinbot", "--config", str(config_path)])
    runtime = _Runtime()

    delta = shinbot_plugin_sleepy._apply_schedule("shinbot_plugin_sleepy", runtime)

    assert delta == 4.5
    assert runtime.calls == [(4.5, "shinbot_plugin_sleepy")]


@pytest.mark.asyncio
async def test_builtin_adapter_plugins_register_provider_schemas(tmp_path: Path) -> None:
    registry = ConfigProviderRegistry()
    mgr = PluginManager(
        CommandRegistry(),
        EventBus(),
        data_dir=tmp_path,
        adapter_manager=AdapterManager(),
        config_provider_registry=registry,
    )

    builtin_root = _repo_path("shinbot", "builtin_plugins")
    for plugin_id, module_path in (
        ("shinbot_adapter_satori", "shinbot.builtin_plugins.shinbot_adapter_satori"),
        ("shinbot_adapter_onebot_v11", "shinbot.builtin_plugins.shinbot_adapter_onebot_v11"),
        ("shinbot_adapter_qqofficial", "shinbot.builtin_plugins.shinbot_adapter_qqofficial"),
    ):
        metadata = json.loads(
            (builtin_root / plugin_id / "metadata.json").read_text(encoding="utf-8")
        )
        await mgr.load_plugin_async(plugin_id, module_path, declared_metadata=metadata)

    adapter_ids = [provider.id for provider in registry.list("adapter")]
    assert adapter_ids == ["onebot_v11", "qqofficial", "satori"]
    assert registry.default_config("adapter", "qqofficial")["app_secret"] == ""
    onebot_provider = registry.get("adapter", "onebot_v11")
    assert onebot_provider is not None
    assert onebot_provider.example_toml.startswith('mode = "reverse"')


@pytest.mark.asyncio
async def test_builtin_search_plugin_registers_provider_schema(tmp_path: Path) -> None:
    registry = ConfigProviderRegistry()
    mgr = PluginManager(
        CommandRegistry(),
        EventBus(),
        data_dir=tmp_path,
        tool_registry=ToolRegistry(),
        config_provider_registry=registry,
    )

    builtin_root = _repo_path("shinbot", "builtin_plugins")
    metadata = json.loads(
        (builtin_root / "shinbot_plugin_search" / "metadata.json").read_text(encoding="utf-8")
    )
    await mgr.load_plugin_async(
        "shinbot_plugin_search",
        "shinbot.builtin_plugins.shinbot_plugin_search",
        declared_metadata=metadata,
    )

    provider = registry.get("plugin", "shinbot_plugin_search")
    assert provider is not None
    assert provider.example_toml.startswith('tavily_api_key = "${TAVILY_API_KEY}"')


@pytest.mark.asyncio
async def test_plugin_reload_refreshes_provider_schema(tmp_path: Path) -> None:
    plugin_id = "demo_reload_plugin"
    schema = """
[provider]
kind = "plugin"
id = "demo_reload_plugin"
display_name = "Demo v1"

[[fields]]
path = "api_key"
type = "string"
default = ""
""".strip()
    plugins_dir = _write_metadata_plugin(tmp_path, plugin_id=plugin_id, schema_body=schema + "\n")
    registry = ConfigProviderRegistry()
    mgr = PluginManager(CommandRegistry(), EventBus(), data_dir=tmp_path, config_provider_registry=registry)

    await mgr.load_plugins_from_metadata_dir_async(plugins_dir)
    provider = registry.get("plugin", plugin_id)
    assert provider is not None
    assert provider.display_name == "Demo v1"

    (plugins_dir / plugin_id / "config.schema.toml").write_text(
        schema.replace("Demo v1", "Demo v2") + "\n",
        encoding="utf-8",
    )

    await mgr.reload_plugin_async(plugin_id)

    provider = registry.get("plugin", plugin_id)
    assert provider is not None
    assert provider.display_name == "Demo v2"


def test_boot_plugin_enabled_lookup_uses_plugin_entry(tmp_path: Path) -> None:
    boot = BootController(config_path=tmp_path / "config.toml", data_dir=tmp_path)
    boot.config = {"plugins": [{"id": "shinbot_plugin_search", "enabled": False}]}
    bot = ShinBot(data_dir=tmp_path)
    bot.config_provider_registry.register(
        load_provider_schema(
            _repo_path(
                "shinbot",
                "builtin_plugins",
                "shinbot_plugin_search",
                "config.schema.toml",
            )
        )
    )
    boot.bot = bot

    assert boot._configured_plugin_enabled("shinbot_plugin_search") is False


@pytest.mark.asyncio
async def test_boot_accepts_loaded_plugin_entries_without_module(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    boot = BootController(config_path=tmp_path / "config.toml", data_dir=tmp_path)
    boot.config = {
        "plugins": [
            {
                "id": "demo-plugin",
                "enabled": True,
                "config": {"api_key": "secret-value"},
            }
        ]
    }
    bot = ShinBot(data_dir=tmp_path)
    module_name = "test_boot_loaded_plugin_no_module"
    sys.modules.pop(module_name, None)
    _make_plugin_module(module_name, setup_fn=lambda plg: None)
    await bot.load_plugin_async("demo-plugin", module_name)
    boot.bot = bot

    try:
        with caplog.at_level("WARNING"):
            await boot._phase4_plugin_loading()
    finally:
        sys.modules.pop(module_name, None)

    assert "Invalid plugin config entry" not in caplog.text
    assert "secret-value" not in caplog.text


class TestPluginManager:
    def setup_method(self):
        self.cmd_reg = CommandRegistry()
        self.event_bus = EventBus()
        self.keyword_registry = KeywordRegistry()
        self.route_table = RouteTable()
        self.route_targets = RouteTargetRegistry()
        self.tool_registry = ToolRegistry()
        self._tmp_data_dir_ctx = tempfile.TemporaryDirectory()
        self._tmp_data_dir = Path(self._tmp_data_dir_ctx.name)
        self.mgr = PluginManager(
            self.cmd_reg,
            self.event_bus,
            keyword_registry=self.keyword_registry,
            route_table=self.route_table,
            route_targets=self.route_targets,
            tool_registry=self.tool_registry,
            data_dir=self._tmp_data_dir,
        )

    def teardown_method(self):
        # Clean up any test modules
        for key in list(sys.modules.keys()):
            if key.startswith("test_plugin_"):
                del sys.modules[key]
        self._tmp_data_dir_ctx.cleanup()

    def test_load_plugin(self):
        def setup(plg: Plugin):
            @plg.on_command("greet")
            async def greet(c, args):
                pass

        _make_plugin_module(
            "test_plugin_greet",
            setup_fn=setup,
            __plugin_name__="Greeter",
            __plugin_version__="1.0.0",
        )

        meta = self.mgr.load_plugin("greet", "test_plugin_greet")
        assert meta.state == PluginState.ACTIVE
        assert meta.name == "greet"
        assert "greet" in meta.commands
        assert self.cmd_reg.get("greet") is not None

    def test_load_duplicate_raises(self):
        _make_plugin_module("test_plugin_dup", setup_fn=lambda plg: None)
        self.mgr.load_plugin("dup", "test_plugin_dup")
        with pytest.raises(ValueError, match="already loaded"):
            self.mgr.load_plugin("dup", "test_plugin_dup")

    def test_load_no_setup_raises(self):
        mod = types.ModuleType("test_plugin_nosetup")
        sys.modules["test_plugin_nosetup"] = mod
        with pytest.raises(AttributeError, match="setup"):
            self.mgr.load_plugin("nosetup", "test_plugin_nosetup")

    def test_unload_plugin(self):
        def setup(plg: Plugin):
            @plg.on_command("bye")
            async def bye(c, args):
                pass

            @plg.on_event("test-event")
            async def on_test(event):
                pass

            @plg.on_keyword("hello")
            async def on_keyword(ctx, match):
                pass

            @plg.on_route(RouteCondition(), rule_id="bye-route", target="bye-target")
            async def on_route(ctx, rule):
                pass

            @plg.tool(
                name="bye_tool",
                description="tool",
                input_schema={"type": "object", "properties": {}},
            )
            async def bye_tool(args, runtime):
                return None

        _make_plugin_module("test_plugin_bye", setup_fn=setup)
        self.mgr.load_plugin("bye", "test_plugin_bye")

        assert self.cmd_reg.get("bye") is not None
        assert self.event_bus.handler_count("test-event") == 1
        assert len(self.keyword_registry.match("hello")) == 1
        assert any(rule.id == "bye-route" for rule in self.route_table.rules)
        assert self.route_targets.get("bye-target") is not None
        assert self.tool_registry.get_tool_by_name("bye_tool") is not None

        result = self.mgr.unload_plugin("bye")
        assert result is True
        assert self.cmd_reg.get("bye") is None
        assert self.event_bus.handler_count("test-event") == 0
        assert self.keyword_registry.match("hello") == []
        assert all(rule.id != "bye-route" for rule in self.route_table.rules)
        assert self.route_targets.get("bye-target") is None
        assert self.tool_registry.get_tool_by_name("bye_tool") is None
        assert self.mgr.get_plugin("bye") is None

    def test_unload_nonexistent(self):
        assert self.mgr.unload_plugin("nonexistent") is False

    def test_disable_plugin_keeps_metadata_and_unregisters_handlers(self):
        def setup(plg: Plugin):
            @plg.on_command("sleep")
            async def sleep(c, args):
                pass

            @plg.on_event("test-event")
            async def on_test(event):
                pass

        _make_plugin_module("test_plugin_disable", setup_fn=setup)
        self.mgr.load_plugin("disable-me", "test_plugin_disable")

        meta = self.mgr.disable_plugin("disable-me")

        assert meta.state == PluginState.DISABLED
        assert self.mgr.get_plugin("disable-me") is meta
        assert self.cmd_reg.get("sleep") is None
        assert self.event_bus.handler_count("test-event") == 0

    def test_enable_plugin_restores_registrations(self):
        def setup(plg: Plugin):
            @plg.on_command("wake")
            async def wake(c, args):
                pass

        _make_plugin_module("test_plugin_enable", setup_fn=setup)
        self.mgr.load_plugin("enable-me", "test_plugin_enable")
        self.mgr.disable_plugin("enable-me")

        meta = self.mgr.enable_plugin("enable-me")

        assert meta.state == PluginState.ACTIVE
        assert self.cmd_reg.get("wake") is not None

    def test_reload_plugin(self):
        call_count = {"n": 0}

        def setup(plg: Plugin):
            call_count["n"] += 1

            @plg.on_command("reload_test")
            async def handler(c, args):
                pass

        _make_plugin_module("test_plugin_reload", setup_fn=setup)
        self.mgr.load_plugin("reload", "test_plugin_reload")
        assert call_count["n"] == 1

        # Re-create module for reload (simulating hot reload)
        _make_plugin_module("test_plugin_reload", setup_fn=setup)
        meta = self.mgr.reload_plugin("reload")
        assert call_count["n"] == 2
        assert meta.state == PluginState.ACTIVE

    def test_teardown_called_on_unload(self):
        torn_down = {"called": False}

        def setup(plg):
            pass

        def teardown():
            torn_down["called"] = True

        _make_plugin_module("test_plugin_teardown", setup_fn=setup, teardown_fn=teardown)
        self.mgr.load_plugin("td", "test_plugin_teardown")
        self.mgr.unload_plugin("td")
        assert torn_down["called"] is True

    def test_all_plugins(self):
        _make_plugin_module("test_plugin_a1", setup_fn=lambda plg: None)
        _make_plugin_module("test_plugin_b1", setup_fn=lambda plg: None)
        self.mgr.load_plugin("a", "test_plugin_a1")
        self.mgr.load_plugin("b", "test_plugin_b1")
        assert len(self.mgr.all_plugins) == 2

    def test_get_plugin(self):
        _make_plugin_module("test_plugin_get1", setup_fn=lambda plg: None)
        self.mgr.load_plugin("get1", "test_plugin_get1")
        assert self.mgr.get_plugin("get1") is not None
        assert self.mgr.get_plugin("nope") is None

    @pytest.mark.asyncio
    async def test_load_plugin_async(self):
        async def async_setup(plg: Plugin):
            @plg.on_command("async_cmd")
            async def handler(c, args):
                pass

        _make_plugin_module("test_plugin_async_load", setup_fn=async_setup)
        meta = await self.mgr.load_plugin_async("async1", "test_plugin_async_load")
        assert meta.state == PluginState.ACTIVE
        assert self.cmd_reg.get("async_cmd") is not None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure_phase", ["setup", "on_enable"])
    async def test_load_failure_rolls_back_all_registered_resources(
        self,
        failure_phase: str,
    ) -> None:
        class _ModelRuntime:
            def __init__(self) -> None:
                self.observers: list[object] = []

            def register_observer(self, observer: object) -> None:
                self.observers.append(observer)

            def unregister_observer(self, observer: object) -> None:
                self.observers = [item for item in self.observers if item is not observer]

        class _CronManager:
            def __init__(self) -> None:
                self.jobs: dict[str, list[str]] = {}
                self.removed_owners: list[str] = []

            def add_cron_job(
                self,
                plugin_id: str,
                func,
                cron_expr: str,
                *,
                timezone: str | None = None,
                job_id: str | None = None,
                description: str = "",
            ) -> str:
                del func, cron_expr, timezone, description
                resolved_job_id = job_id or f"{plugin_id}-job"
                self.jobs.setdefault(plugin_id, []).append(resolved_job_id)
                return resolved_job_id

            def remove_jobs(self, plugin_id: str) -> int:
                self.removed_owners.append(plugin_id)
                return len(self.jobs.pop(plugin_id, []))

        command_registry = CommandRegistry()
        event_bus = EventBus()
        keyword_registry = KeywordRegistry()
        route_table = RouteTable()
        route_targets = RouteTargetRegistry()
        tool_registry = ToolRegistry()
        adapter_manager = AdapterManager()
        model_runtime = _ModelRuntime()
        cron_manager = _CronManager()
        manager = PluginManager(
            command_registry,
            event_bus,
            keyword_registry=keyword_registry,
            route_table=route_table,
            route_targets=route_targets,
            adapter_manager=adapter_manager,
            tool_registry=tool_registry,
            model_runtime=model_runtime,
            cron_manager=cron_manager,
            data_dir=self._tmp_data_dir,
        )
        boot = types.SimpleNamespace(data_dir=self._tmp_data_dir, bot=None)
        manager._boot = boot

        task_started = asyncio.Event()
        task_cancelled = asyncio.Event()
        background_tasks: list[asyncio.Task[object]] = []
        plugin_contexts: list[Plugin] = []

        async def background_worker() -> None:
            task_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                task_cancelled.set()

        def observer(event: dict[str, object]) -> None:
            del event

        async def install_plugin(*args, **kwargs) -> None:
            del args, kwargs

        async def setup(plg: Plugin) -> None:
            plugin_contexts.append(plg)

            @plg.on_command("partial_command")
            async def command_handler(ctx, args):
                pass

            @plg.on_event("partial-event")
            async def event_handler(event):
                pass

            @plg.on_keyword("partial-keyword")
            async def keyword_handler(ctx, match):
                pass

            @plg.on_route(
                RouteCondition(),
                rule_id="partial-route",
                target="partial-target",
            )
            async def route_handler(ctx, rule):
                pass

            @plg.tool(
                name="partial_tool",
                description="Partial tool",
                input_schema={"type": "object", "properties": {}},
            )
            async def partial_tool(arguments, runtime):
                return None

            @plg.on_cron("* * * * *", job_id="partial-cron")
            async def cron_handler() -> None:
                pass

            plg.register_adapter_factory("partial-platform", lambda **kwargs: kwargs)
            plg.register_model_runtime_observer(observer)
            plg.register_marketplace_source(
                source_id="partial-source",
                name="Partial source",
                repository_url="https://example.invalid/partial.git",
            )
            plg.register_plugin_installer("partial-installer", install_plugin)
            background_tasks.append(plg.create_task(background_worker(), name="worker"))
            await task_started.wait()
            if failure_phase == "setup":
                raise RuntimeError("setup failed")

        async def on_enable(plg: Plugin) -> None:
            del plg
            if failure_phase == "on_enable":
                raise RuntimeError("on_enable failed")

        _make_plugin_module(
            f"test_plugin_partial_{failure_phase}",
            setup_fn=setup,
            on_enable=on_enable,
        )

        with pytest.raises(RuntimeError, match=f"{failure_phase} failed"):
            await manager.load_plugin_async(
                "partial-plugin",
                f"test_plugin_partial_{failure_phase}",
            )

        marketplace = boot.plugin_marketplace_service
        assert command_registry.get("partial_command") is None
        assert event_bus.handler_count("partial-event") == 0
        assert keyword_registry.match("partial-keyword") == []
        assert all(rule.id != "partial-route" for rule in route_table.rules)
        assert route_targets.get("partial-target") is None
        assert tool_registry.get_tool_by_name("partial_tool") is None
        assert "partial-platform" not in adapter_manager.registered_platforms
        assert model_runtime.observers == []
        assert cron_manager.jobs == {}
        assert cron_manager.removed_owners == ["partial-plugin"]
        assert "partial-source" not in marketplace.sources
        assert marketplace.get_installer("partial-installer") is None
        assert background_tasks[0].cancelled()
        assert task_cancelled.is_set()
        assert plugin_contexts[0].background_tasks == frozenset()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure_phase", ["setup", "on_enable"])
    async def test_load_failure_freezes_context_without_background_tasks(
        self,
        failure_phase: str,
    ) -> None:
        plugin_contexts: list[Plugin] = []

        async def setup(plg: Plugin) -> None:
            plugin_contexts.append(plg)
            if failure_phase == "setup":
                raise RuntimeError("setup failed")

        async def on_enable(plg: Plugin) -> None:
            del plg
            if failure_phase == "on_enable":
                raise RuntimeError("on_enable failed")

        module_name = f"test_plugin_frozen_{failure_phase}"
        _make_plugin_module(
            module_name,
            setup_fn=setup,
            on_enable=on_enable,
        )

        with pytest.raises(RuntimeError, match=f"{failure_phase} failed"):
            await self.mgr.load_plugin_async("frozen-plugin", module_name)

        plugin = plugin_contexts[0]
        assert plugin.task_creation_frozen is True

        async def late_worker() -> None:
            await asyncio.sleep(0)

        coroutine = late_worker()
        with pytest.raises(RuntimeError, match="during deactivation"):
            plugin.create_task(coroutine)
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cancel_phase", ["setup", "on_enable"])
    async def test_cancelled_load_restores_override_and_freezes_context(
        self,
        cancel_phase: str,
    ) -> None:
        plugin_id = "cancelled-load"
        module_name = f"test_plugin_cancelled_load_{cancel_phase}"
        phase_started = asyncio.Event()
        plugin_contexts: list[Plugin] = []

        async def original_handler(_ctx, _args) -> None:
            pass

        original = CommandDef(
            name="shared-command",
            handler=original_handler,
            owner="original-owner",
        )
        self.cmd_reg.register(original)

        async def setup(plg: Plugin) -> None:
            plugin_contexts.append(plg)

            @plg.on_command("shared-command")
            async def override_handler(_ctx, _args) -> None:
                pass

            if cancel_phase == "setup":
                phase_started.set()
                await asyncio.Event().wait()

        async def on_enable(_plg: Plugin) -> None:
            if cancel_phase == "on_enable":
                phase_started.set()
                await asyncio.Event().wait()

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            on_enable=on_enable,
        )

        load_task = asyncio.create_task(
            self.mgr.load_plugin_async(plugin_id, module_name)
        )
        await phase_started.wait()
        assert self.cmd_reg.get("shared-command") is not original

        load_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await load_task

        assert self.cmd_reg.get("shared-command") is original
        assert plugin_contexts[0].task_creation_frozen is True
        assert self.mgr.get_plugin(plugin_id) is None
        assert not self.route_targets.accepts_tasks(plugin_id)

    @pytest.mark.asyncio
    async def test_concurrent_load_for_same_plugin_runs_setup_once(self) -> None:
        plugin_id = "concurrent-load"
        module_name = "test_plugin_concurrent_load"
        setup_started = asyncio.Event()
        allow_setup_to_finish = asyncio.Event()
        setup_calls = 0

        async def setup(plg: Plugin) -> None:
            nonlocal setup_calls
            setup_calls += 1

            @plg.on_command("concurrent-command")
            async def handler(_ctx, _args) -> None:
                pass

            setup_started.set()
            await allow_setup_to_finish.wait()

        _make_plugin_module(module_name, setup_fn=setup)

        first_load = asyncio.create_task(
            self.mgr.load_plugin_async(plugin_id, module_name)
        )
        await setup_started.wait()
        second_load = asyncio.create_task(
            self.mgr.load_plugin_async(plugin_id, module_name)
        )
        await asyncio.sleep(0)

        assert setup_calls == 1
        assert not second_load.done()

        allow_setup_to_finish.set()
        meta = await first_load
        with pytest.raises(ValueError, match="already loaded"):
            await second_load

        assert meta.state == PluginState.ACTIVE
        assert setup_calls == 1
        command = self.cmd_reg.get("concurrent-command")
        assert command is not None
        assert command.owner == plugin_id

    @pytest.mark.asyncio
    async def test_cancelled_disable_finishes_cleanup_and_commits_state(self) -> None:
        plugin_id = "cancelled-disable"
        module_name = "test_plugin_cancelled_disable"
        disable_started = asyncio.Event()
        teardown_called = asyncio.Event()

        def setup(plg: Plugin) -> None:
            @plg.on_command("cancelled-disable-command")
            async def handler(_ctx, _args) -> None:
                pass

        async def on_disable(_plg: Plugin) -> None:
            disable_started.set()
            await asyncio.Event().wait()

        def teardown() -> None:
            teardown_called.set()

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            on_disable=on_disable,
            teardown_fn=teardown,
        )
        meta = await self.mgr.load_plugin_async(plugin_id, module_name)

        disable_task = asyncio.create_task(self.mgr.disable_plugin_async(plugin_id))
        await disable_started.wait()
        disable_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await disable_task

        assert meta.state == PluginState.DISABLED
        assert self.cmd_reg.get("cancelled-disable-command") is None
        assert teardown_called.is_set()
        assert not self.route_targets.accepts_tasks(plugin_id)

    @pytest.mark.asyncio
    async def test_handler_can_disable_its_own_plugin_without_self_await(self) -> None:
        plugin_id = "self-disabling"
        module_name = "test_plugin_self_disabling"
        bot = ShinBot(data_dir=self._tmp_data_dir)
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("self-disable-test", "mock")
        handler_finished = asyncio.Event()

        def setup(plg: Plugin) -> None:
            @plg.on_command("self-disable")
            async def handler(_ctx, _args) -> None:
                meta = await bot.plugin_manager.disable_plugin_async(plugin_id)
                assert meta.state == PluginState.DISABLED
                handler_finished.set()

        _make_plugin_module(module_name, setup_fn=setup)
        await bot.plugin_manager.load_plugin_async(plugin_id, module_name)

        await bot.message_ingress.process_event(
            make_message_event(content="/self-disable", instance_id="self-disable-test"),
            adapter,
        )
        await asyncio.wait_for(handler_finished.wait(), timeout=1.0)
        await asyncio.sleep(0)

        meta = bot.plugin_manager.get_plugin(plugin_id)
        assert meta is not None
        assert meta.state == PluginState.DISABLED
        assert bot.command_registry.get("self-disable") is None
        assert bot.route_targets.pending_task_count_for_owner(plugin_id) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["disable", "unload"])
    async def test_background_task_can_deactivate_its_own_plugin(
        self,
        operation: str,
    ) -> None:
        plugin_id = f"background-self-{operation}"
        module_name = f"test_plugin_background_self_{operation}"
        worker_ready = asyncio.Event()
        start_lifecycle = asyncio.Event()
        lifecycle_finished = asyncio.Event()
        plugin_contexts: list[Plugin] = []
        background_tasks: list[asyncio.Task[None]] = []

        async def lifecycle_worker() -> None:
            worker_ready.set()
            await start_lifecycle.wait()
            if operation == "disable":
                meta = await self.mgr.disable_plugin_async(plugin_id)
                assert meta.state == PluginState.DISABLED
            else:
                assert await self.mgr.unload_plugin_async(plugin_id) is True
            lifecycle_finished.set()

        def setup(plg: Plugin) -> None:
            plugin_contexts.append(plg)

            @plg.on_command("background-owned-command")
            async def handler(_ctx, _args) -> None:
                pass

            background_tasks.append(
                plg.create_task(lifecycle_worker(), name="self-deactivate")
            )

        _make_plugin_module(module_name, setup_fn=setup)
        await self.mgr.load_plugin_async(plugin_id, module_name)
        await worker_ready.wait()

        start_lifecycle.set()
        await asyncio.wait_for(lifecycle_finished.wait(), timeout=1.0)
        await asyncio.sleep(0)

        assert self.cmd_reg.get("background-owned-command") is None
        assert background_tasks[0].done()
        assert plugin_contexts[0].background_tasks == frozenset()
        if operation == "disable":
            meta = self.mgr.get_plugin(plugin_id)
            assert meta is not None
            assert meta.state == PluginState.DISABLED
        else:
            assert self.mgr.get_plugin(plugin_id) is None

    @pytest.mark.asyncio
    async def test_unload_unregisters_adapter_factories(self) -> None:
        adapter_manager = AdapterManager()
        manager = PluginManager(
            CommandRegistry(),
            EventBus(),
            adapter_manager=adapter_manager,
            data_dir=self._tmp_data_dir,
        )

        def setup(plg: Plugin) -> None:
            plg.register_adapter_factory("owned-platform", lambda **kwargs: kwargs)

        _make_plugin_module("test_plugin_adapter_factory", setup_fn=setup)

        await manager.load_plugin_async("adapter-owner", "test_plugin_adapter_factory")
        assert "owned-platform" in adapter_manager.registered_platforms

        assert await manager.unload_plugin_async("adapter-owner") is True
        assert "owned-platform" not in adapter_manager.registered_platforms

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["disable_enable", "reload"])
    async def test_adapter_owner_lifecycle_rebuilds_running_instance(
        self,
        operation: str,
    ) -> None:
        plugin_id = f"adapter-lifecycle-{operation}"
        module_name = f"test_plugin_adapter_lifecycle_{operation}"
        route_targets = RouteTargetRegistry()
        adapter_manager = AdapterManager()
        manager = PluginManager(
            CommandRegistry(),
            EventBus(task_supervisor=route_targets),
            route_targets=route_targets,
            adapter_manager=adapter_manager,
            data_dir=self._tmp_data_dir,
        )
        setup_generation = 0
        created_adapters: list[MockAdapter] = []
        lifecycle: list[tuple[str, int, bool]] = []
        startup_callbacks: list[tuple[int, bool]] = []

        async def startup_callback(generation: int) -> None:
            startup_callbacks.append(
                (generation, route_targets.accepts_tasks(plugin_id))
            )

        def setup(plg: Plugin) -> None:
            nonlocal setup_generation
            setup_generation += 1
            generation = setup_generation

            class OwnedAdapter(MockAdapter):
                async def start(self) -> None:
                    lifecycle.append(
                        ("start", generation, route_targets.accepts_tasks(plugin_id))
                    )
                    if self._event_callback is not None:
                        result = self._event_callback(generation)
                        if inspect.isawaitable(result):
                            await result
                    await super().start()

                async def shutdown(self) -> None:
                    lifecycle.append(
                        ("shutdown", generation, route_targets.accepts_tasks(plugin_id))
                    )
                    await super().shutdown()

            def factory(instance_id: str, platform: str, **_kwargs: object) -> MockAdapter:
                adapter = OwnedAdapter(instance_id=instance_id, platform=platform)
                created_adapters.append(adapter)
                return adapter

            plg.register_adapter_factory("owned-platform", factory)

        def on_disable(_plg: Plugin) -> None:
            lifecycle.append(
                ("on_disable", setup_generation, route_targets.accepts_tasks(plugin_id))
            )

        def teardown() -> None:
            lifecycle.append(
                ("teardown", setup_generation, route_targets.accepts_tasks(plugin_id))
            )

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            on_disable=on_disable,
            teardown_fn=teardown,
        )
        await manager.load_plugin_async(plugin_id, module_name)
        original = adapter_manager.create_instance(
            "owned-1",
            "owned-platform",
            event_callback=startup_callback,
        )
        await adapter_manager.start_instance("owned-1")

        if operation == "disable_enable":
            await manager.disable_plugin_async(plugin_id)
            assert adapter_manager.get_instance("owned-1") is None
            assert adapter_manager.has_instance_spec("owned-1")
            await manager.enable_plugin_async(plugin_id)
        else:
            await manager.reload_plugin_async(plugin_id)

        restored = adapter_manager.get_instance("owned-1")
        assert restored is not None
        assert restored is not original
        assert adapter_manager.is_running("owned-1")
        assert created_adapters == [original, restored]
        assert lifecycle == [
            ("start", 1, True),
            ("on_disable", 1, False),
            ("shutdown", 1, False),
            ("teardown", 1, False),
            ("start", 2, True),
        ]
        assert startup_callbacks == [(1, True), (2, True)]

        assert await manager.unload_plugin_async(plugin_id) is True
        assert adapter_manager.get_instance("owned-1") is None
        assert not adapter_manager.has_instance_spec("owned-1")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["disable", "unload", "reload"])
    async def test_adapter_shutdown_failure_still_commits_plugin_deactivation(
        self,
        operation: str,
    ) -> None:
        plugin_id = f"adapter-shutdown-failure-{operation}"
        module_name = f"test_plugin_adapter_shutdown_failure_{operation}"
        route_targets = RouteTargetRegistry()
        command_registry = CommandRegistry()
        adapter_manager = AdapterManager()
        manager = PluginManager(
            command_registry,
            EventBus(task_supervisor=route_targets),
            route_targets=route_targets,
            adapter_manager=adapter_manager,
            data_dir=self._tmp_data_dir,
        )
        setup_generation = 0
        created_adapters: list[MockAdapter] = []
        callback_events: list[str] = []
        disabled_generations: list[int] = []
        teardown_generations: list[int] = []

        def setup(plg: Plugin) -> None:
            nonlocal setup_generation
            setup_generation += 1
            generation = setup_generation

            @plg.on_command("owned-command")
            async def owned_command(_ctx, _args) -> None:
                return None

            class OwnedAdapter(MockAdapter):
                def __init__(self, instance_id: str, platform: str):
                    super().__init__(instance_id, platform)
                    self.shutdown_attempted = False
                    self.allow_shutdown = generation != 1

                async def shutdown(self) -> None:
                    self.shutdown_attempted = True
                    if not self.allow_shutdown:
                        raise RuntimeError("shutdown failed")
                    await super().shutdown()

            def factory(
                instance_id: str,
                platform: str,
                **_kwargs: object,
            ) -> MockAdapter:
                adapter = OwnedAdapter(instance_id, platform)
                created_adapters.append(adapter)
                return adapter

            plg.register_adapter_factory("owned-platform", factory)

        def on_disable(_plg: Plugin) -> None:
            disabled_generations.append(setup_generation)

        def teardown() -> None:
            teardown_generations.append(setup_generation)

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            on_disable=on_disable,
            teardown_fn=teardown,
        )
        await manager.load_plugin_async(plugin_id, module_name)
        original = adapter_manager.create_instance(
            "owned-1",
            "owned-platform",
            event_callback=callback_events.append,
        )
        await adapter_manager.start_instance("owned-1")

        if operation == "disable":
            meta = await manager.disable_plugin_async(plugin_id)

            assert meta.state == PluginState.DISABLED
            assert manager.get_plugin(plugin_id) is meta
            assert adapter_manager.has_instance_spec("owned-1")
            assert not route_targets.accepts_tasks(plugin_id)
            assert command_registry.get("owned-command") is None

            with pytest.raises(RuntimeError, match="previous adapter is still active"):
                await manager.enable_plugin_async(plugin_id)
            assert meta.state == PluginState.DISABLED
            assert manager.get_plugin(plugin_id) is meta
            assert len(created_adapters) == 1

            original.allow_shutdown = True  # type: ignore[attr-defined]
            enabled = await manager.enable_plugin_async(plugin_id)
            assert enabled.state == PluginState.ACTIVE
        elif operation == "unload":
            assert await manager.unload_plugin_async(plugin_id) is True

            assert manager.get_plugin(plugin_id) is None
            assert adapter_manager.has_instance_spec("owned-1")
            assert not route_targets.accepts_tasks(plugin_id)
            assert command_registry.get("owned-command") is None
        else:
            with pytest.raises(RuntimeError, match="previous adapter is still active"):
                await manager.reload_plugin_async(plugin_id)
            assert manager.get_plugin(plugin_id) is None
            assert adapter_manager.has_instance_spec("owned-1")
            assert not route_targets.accepts_tasks(plugin_id)
            assert command_registry.get("owned-command") is None
            assert len(created_adapters) == 1

            original.allow_shutdown = True  # type: ignore[attr-defined]
            reloaded = await manager.load_plugin_async(plugin_id, module_name)
            assert reloaded.state == PluginState.ACTIVE
            assert manager.get_plugin(plugin_id) is reloaded

        assert disabled_generations == [1]
        assert teardown_generations == [1]
        assert original.shutdown_attempted  # type: ignore[attr-defined]
        assert adapter_manager.get_instance("owned-1") is not original
        assert original._event_callback is not None
        assert await original._event_callback("stale") is None
        assert callback_events == []

        if operation != "unload":
            restored = adapter_manager.get_instance("owned-1")
            assert restored is not None
            assert adapter_manager.is_running("owned-1")
            assert route_targets.accepts_tasks(plugin_id)
            assert command_registry.get("owned-command") is not None
            assert restored._event_callback is not None
            assert await restored._event_callback("fresh") is None
            assert callback_events == ["fresh"]
            assert await manager.unload_plugin_async(plugin_id) is True
        else:
            original.allow_shutdown = True  # type: ignore[attr-defined]
            assert await adapter_manager.delete_instance("owned-1") is True
            assert not adapter_manager.has_instance_spec("owned-1")

        assert len(created_adapters) == (1 if operation == "unload" else 2)

    @pytest.mark.asyncio
    async def test_failed_adapter_plugin_setup_drains_new_owner_instances(self) -> None:
        plugin_id = "adapter-setup-failure"
        module_name = "test_plugin_adapter_setup_failure"
        route_targets = RouteTargetRegistry()
        adapter_manager = AdapterManager()
        manager = PluginManager(
            CommandRegistry(),
            EventBus(task_supervisor=route_targets),
            route_targets=route_targets,
            adapter_manager=adapter_manager,
            data_dir=self._tmp_data_dir,
        )
        created: list[MockAdapter] = []

        async def setup(plg: Plugin) -> None:
            def factory(instance_id: str, platform: str, **_kwargs: object) -> MockAdapter:
                adapter = MockAdapter(instance_id=instance_id, platform=platform)
                created.append(adapter)
                return adapter

            plg.register_adapter_factory("partial-platform", factory)
            adapter_manager.create_instance("partial-1", "partial-platform")
            await adapter_manager.start_instance("partial-1")
            raise RuntimeError("setup failed")

        _make_plugin_module(module_name, setup_fn=setup)

        with pytest.raises(RuntimeError, match="setup failed"):
            await manager.load_plugin_async(plugin_id, module_name)

        assert created[0].stopped is True
        assert adapter_manager.get_instance("partial-1") is None
        assert not adapter_manager.has_instance_spec("partial-1")
        assert "partial-platform" not in adapter_manager.registered_platforms

    @pytest.mark.asyncio
    async def test_disable_waits_for_route_tasks_and_enable_resumes_owner(self) -> None:
        plugin_id = "route-owner"
        module_name = "test_plugin_route_owner"
        bot = ShinBot(data_dir=self._tmp_data_dir)
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("route-test", "mock")
        first_started = asyncio.Event()
        first_cancelled = asyncio.Event()
        second_started = asyncio.Event()
        setup_generation = 0
        lifecycle_observations: list[tuple[str, bool, int]] = []

        def setup(plg: Plugin) -> None:
            nonlocal setup_generation
            setup_generation += 1
            generation = setup_generation

            @plg.on_route(
                RouteCondition(event_types=frozenset({"message-created"})),
                rule_id="route-owner-rule",
                target="route-owner-target",
            )
            async def route_handler(ctx, rule) -> None:
                del ctx, rule
                if generation == 1:
                    first_started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        first_cancelled.set()
                        raise
                second_started.set()

        async def on_disable(plg: Plugin) -> None:
            del plg
            lifecycle_observations.append(
                (
                    "on_disable",
                    first_cancelled.is_set(),
                    bot.route_targets.pending_task_count_for_owner(plugin_id),
                )
            )

        def teardown() -> None:
            lifecycle_observations.append(
                (
                    "teardown",
                    first_cancelled.is_set(),
                    bot.route_targets.pending_task_count_for_owner(plugin_id),
                )
            )

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            teardown_fn=teardown,
            on_disable=on_disable,
        )
        await bot.plugin_manager.load_plugin_async(plugin_id, module_name)

        await bot.message_ingress.process_event(
            make_message_event(content="first", instance_id="route-test"),
            adapter,
        )
        await first_started.wait()
        assert bot.route_targets.pending_task_count_for_owner(plugin_id) == 1

        meta = await bot.plugin_manager.disable_plugin_async(plugin_id)

        assert meta.state == PluginState.DISABLED
        assert lifecycle_observations == [
            ("on_disable", True, 0),
            ("teardown", True, 0),
        ]
        assert bot.route_targets.pending_task_count_for_owner(plugin_id) == 0
        assert not bot.route_targets.accepts_tasks(plugin_id)

        enabled = await bot.plugin_manager.enable_plugin_async(plugin_id)
        assert enabled.state == PluginState.ACTIVE
        assert bot.route_targets.accepts_tasks(plugin_id)

        await bot.message_ingress.process_event(
            make_message_event(content="second", instance_id="route-test"),
            adapter,
        )
        await second_started.wait()

        assert setup_generation == 2
        await bot.plugin_manager.unload_plugin_async(plugin_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("handler_kind", ["command", "keyword", "event"])
    async def test_disable_cancels_framework_dispatched_owned_handler(
        self,
        handler_kind: str,
    ) -> None:
        plugin_id = f"owned-{handler_kind}"
        module_name = f"test_plugin_owned_{handler_kind}"
        bot = ShinBot(data_dir=self._tmp_data_dir)
        bot.adapter_manager.register_adapter("mock", MockAdapter)
        adapter = bot.add_adapter("owned-test", "mock")
        handler_started = asyncio.Event()
        handler_cancelled = asyncio.Event()
        lifecycle_observations: list[tuple[str, bool, int]] = []

        async def owned_handler(*_args: object) -> None:
            handler_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                handler_cancelled.set()
                raise

        def setup(plg: Plugin) -> None:
            if handler_kind == "command":
                plg.on_command("owned-command")(owned_handler)
            elif handler_kind == "keyword":
                plg.on_keyword("owned-keyword")(owned_handler)
            else:
                plg.on_event("owned-event")(owned_handler)

        async def on_disable(plg: Plugin) -> None:
            del plg
            lifecycle_observations.append(
                (
                    "on_disable",
                    handler_cancelled.is_set(),
                    bot.route_targets.pending_task_count_for_owner(plugin_id),
                )
            )

        def teardown() -> None:
            lifecycle_observations.append(
                (
                    "teardown",
                    handler_cancelled.is_set(),
                    bot.route_targets.pending_task_count_for_owner(plugin_id),
                )
            )

        _make_plugin_module(
            module_name,
            setup_fn=setup,
            teardown_fn=teardown,
            on_disable=on_disable,
        )
        await bot.plugin_manager.load_plugin_async(plugin_id, module_name)

        event_task: asyncio.Task[list[object]] | None = None
        if handler_kind == "event":
            event_task = asyncio.create_task(bot.event_bus.emit("owned-event", object()))
        else:
            content = "/owned-command" if handler_kind == "command" else "owned-keyword"
            await bot.message_ingress.process_event(
                make_message_event(content=content, instance_id="owned-test"),
                adapter,
            )
        await handler_started.wait()
        assert bot.route_targets.pending_task_count_for_owner(plugin_id) == 1

        await bot.plugin_manager.disable_plugin_async(plugin_id)
        if event_task is not None:
            await asyncio.gather(event_task, return_exceptions=True)

        assert lifecycle_observations == [
            ("on_disable", True, 0),
            ("teardown", True, 0),
        ]
        assert bot.route_targets.pending_task_count_for_owner(plugin_id) == 0
        await bot.plugin_manager.unload_plugin_async(plugin_id)


# ── _topo_sort unit tests ──────────────────────────────────────────────


def _fake_candidates(specs: list[tuple[str, list[str]]]) -> list[tuple[Path, dict]]:
    return [
        (Path(f"/fake/{pid}"), {"id": pid, "dependencies": deps, "entry": "__init__.py"})
        for pid, deps in specs
    ]


class TestTopoSort:
    def test_no_dependencies_preserves_order(self):
        candidates = _fake_candidates([("a", []), ("b", []), ("c", [])])
        ids = [m["id"] for _, m in _topo_sort(candidates)]
        assert ids == ["a", "b", "c"]

    def test_simple_dependency_chain(self):
        # b depends on a → a must come before b
        candidates = _fake_candidates([("b", ["a"]), ("a", [])])
        ids = [m["id"] for _, m in _topo_sort(candidates)]
        assert ids.index("a") < ids.index("b")

    def test_diamond_dependency(self):
        # c and d both depend on b; b depends on a
        candidates = _fake_candidates([("c", ["b"]), ("d", ["b"]), ("b", ["a"]), ("a", [])])
        ids = [m["id"] for _, m in _topo_sort(candidates)]
        assert ids.index("a") < ids.index("b")
        assert ids.index("b") < ids.index("c")
        assert ids.index("b") < ids.index("d")

    def test_external_dependency_silently_skipped(self):
        # b depends on "external" which is not in the candidate set
        candidates = _fake_candidates([("b", ["external"]), ("a", [])])
        ids = [m["id"] for _, m in _topo_sort(candidates)]
        assert "a" in ids and "b" in ids

    def test_cycle_includes_all_plugins(self):
        # a ↔ b cycle: both should still appear in the result
        candidates = _fake_candidates([("a", ["b"]), ("b", ["a"])])
        ids = [m["id"] for _, m in _topo_sort(candidates)]
        assert set(ids) == {"a", "b"}

    def test_empty_candidates(self):
        assert _topo_sort([]) == []


# ── dependency-order integration test ─────────────────────────────────


@pytest.mark.asyncio
async def test_load_respects_dependency_order(tmp_path: Path):
    """plugin_b (depends on plugin_z) must load after plugin_z even though
    alphabetical order would place b before z."""
    load_order: list[str] = []

    def _make_setup(pid: str):
        def setup(plg: Plugin):
            load_order.append(pid)

        return setup

    prefix = tmp_path.name

    # plugin_a — no deps
    # plugin_b — depends on plugin_z
    # plugin_z — no deps
    # Alphabetical: a, b, z → b would load before z (wrong).
    # Topo-sorted:  a, z, b (z is b's dep, so z first).
    for pid, deps in [("plugin_a", []), ("plugin_b", ["plugin_z"]), ("plugin_z", [])]:
        plugin_dir = tmp_path / pid
        plugin_dir.mkdir()
        (plugin_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "id": pid,
                    "name": pid,
                    "version": "1.0.0",
                    "author": "test",
                    "description": "",
                    "entry": "__init__.py",
                    "permissions": [],
                    "dependencies": deps,
                }
            )
        )
        (plugin_dir / "__init__.py").write_text("")
        mod = types.ModuleType(f"{prefix}.{pid}")
        mod.setup = _make_setup(pid)  # type: ignore[attr-defined]
        sys.modules[f"{prefix}.{pid}"] = mod

    parent = str(tmp_path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    cmd_reg = CommandRegistry()
    event_bus = EventBus()
    mgr = PluginManager(cmd_reg, event_bus, data_dir=tmp_path)
    await mgr.load_plugins_from_metadata_dir_async(tmp_path)

    assert load_order.index("plugin_z") < load_order.index("plugin_b")
    assert "plugin_a" in load_order


@pytest.mark.asyncio
async def test_rescan_installs_pyproject_dependencies_for_already_loaded_plugin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    plugins_dir = _write_metadata_plugin(tmp_path, plugin_id="shinbot_plugin_dependency_demo")
    plugin_root = plugins_dir / "shinbot_plugin_dependency_demo"

    mgr = PluginManager(CommandRegistry(), EventBus(), data_dir=tmp_path)
    loaded = await mgr.load_plugins_from_metadata_dir_async(plugins_dir)
    assert [meta.id for meta in loaded] == ["shinbot_plugin_dependency_demo"]

    (plugin_root / "pyproject.toml").write_text(
        "\n".join(
            [
                "[project]",
                'name = "shinbot-plugin-dependency-demo"',
                'version = "1.0.0"',
                "dependencies = [",
                '    "cairosvg>=2.7.0",',
                "]",
                "",
            ]
        ),
        encoding="utf-8",
    )
    captured: list[tuple[str, ...]] = []

    class _FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"installed", b""

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured.append(tuple(str(arg) for arg in args))
        return _FakeProcess()

    monkeypatch.setattr(
        plugin_dependencies.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    loaded_again = await mgr.load_plugins_from_metadata_dir_async(plugins_dir)

    assert loaded_again == []
    assert captured == [
        (
            sys.executable,
            "-m",
            "pip",
            "install",
            "cairosvg>=2.7.0",
        )
    ]


@pytest.mark.asyncio
async def test_load_all_async_includes_builtin_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    builtin_root = tmp_path / "builtin_plugins"
    builtin_plugin_dir = builtin_root / "shinbot_plugin_builtin_demo"
    builtin_plugin_dir.mkdir(parents=True)
    (builtin_plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": "shinbot_plugin_builtin_demo",
                "name": "builtin-demo",
                "version": "1.0.0",
                "author": "test",
                "description": "",
                "entry": "__init__.py",
                "permissions": [],
            }
        )
    )
    (builtin_plugin_dir / "__init__.py").write_text("")

    mod = types.ModuleType("shinbot.builtin_plugins.shinbot_plugin_builtin_demo")
    mod.setup = lambda plg: None  # type: ignore[attr-defined]
    sys.modules["shinbot.builtin_plugins.shinbot_plugin_builtin_demo"] = mod

    monkeypatch.setattr("shinbot.core.plugins.manager._BUILTIN_PLUGINS_DIR", builtin_root)

    cmd_reg = CommandRegistry()
    event_bus = EventBus()
    mgr = PluginManager(cmd_reg, event_bus, data_dir=tmp_path)

    loaded = await mgr.load_all_async(tmp_path / "user_plugins")

    assert [item.id for item in loaded] == ["shinbot_plugin_builtin_demo"]
    assert mgr.get_plugin("shinbot_plugin_builtin_demo") is not None


@pytest.mark.asyncio
async def test_load_all_async_disables_builtin_default_disabled_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    builtin_root = tmp_path / "builtin_plugins"
    builtin_plugin_dir = builtin_root / "shinbot_debug_builtin_demo"
    builtin_plugin_dir.mkdir(parents=True)
    (builtin_plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": "shinbot_debug_builtin_demo",
                "name": "builtin-debug-demo",
                "version": "1.0.0",
                "author": "test",
                "description": "",
                "entry": "__init__.py",
                "role": "logic",
                "default_enabled": False,
                "permissions": [],
            }
        ),
        encoding="utf-8",
    )
    (builtin_plugin_dir / "__init__.py").write_text("", encoding="utf-8")

    module_name = "shinbot.builtin_plugins.shinbot_debug_builtin_demo"
    mod = types.ModuleType(module_name)

    def setup(plg: Plugin) -> None:
        @plg.on_command("debug_demo")
        async def debug_demo(ctx, args):
            return None

    mod.setup = setup  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module_name, mod)
    monkeypatch.setattr("shinbot.core.plugins.manager._BUILTIN_PLUGINS_DIR", builtin_root)

    cmd_reg = CommandRegistry()
    event_bus = EventBus()
    mgr = PluginManager(cmd_reg, event_bus, data_dir=tmp_path)

    loaded = await mgr.load_all_async(tmp_path / "user_plugins")

    assert [item.id for item in loaded] == ["shinbot_debug_builtin_demo"]
    assert loaded[0].state == PluginState.DISABLED
    assert mgr.get_plugin("shinbot_debug_builtin_demo").state == PluginState.DISABLED
    assert cmd_reg.get("debug_demo") is None


@pytest.mark.asyncio
async def test_preregister_model_runtime_extensions_registers_backend_and_provider(
    tmp_path: Path,
) -> None:
    plugin_id = "demo_runtime_extensions"
    module_body = "\n".join(
        [
            "from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan",
            "from shinbot.agent.services.model_runtime.providers import ModelProviderDescriptor",
            "",
            "class DemoBackend:",
            '    name = "demo_backend"',
            "",
            "    def plan_request(self, *, provider, model, call, timeout_override, operation):",
            "        return BackendRequestPlan(",
            "            operation=operation,",
            '            payload={"model": model["backend_model"]},',
            '            safe_payload={"model": model["backend_model"]},',
            "            backend_name=self.name,",
            '            backend_model=str(model["backend_model"]),',
            "        )",
            "",
            "    def invoke(self, plan):",
            '        return {"choices": [{"message": {"content": "ok"}}], "usage": {}}',
            "",
            "    def normalize_response(self, *, operation, response, usage):",
            '        return {"text": "ok", "tool_calls": []}',
            "",
            "def register_model_runtime_extensions(registrar):",
            '    registrar.register_backend_factory("demo_backend", DemoBackend)',
            "    registrar.register_provider_descriptor(",
            "        ModelProviderDescriptor(",
            '            provider_type="demo_runtime_provider",',
            '            supported_backends=frozenset({"demo_backend"}),',
            '            auth_strategy="none",',
            '            catalog_path=None,',
            "        )",
            "    )",
            "",
            "def setup(plg):",
            "    pass",
        ]
    )
    plugins_dir = _write_metadata_plugin(
        tmp_path,
        plugin_id=plugin_id,
        module_body=module_body,
    )
    mgr = PluginManager(CommandRegistry(), EventBus(), data_dir=tmp_path)

    await mgr.preregister_model_runtime_extensions(plugins_dir)

    from shinbot.agent.services.model_runtime.backends import create_registered_backend
    from shinbot.agent.services.model_runtime.providers import require_provider_descriptor

    backend = create_registered_backend("demo_backend")
    descriptor = require_provider_descriptor("demo_runtime_provider")
    assert backend.name == "demo_backend"
    assert descriptor.supports_backend("demo_backend")


@pytest.mark.asyncio
async def test_metadata_identity_overrides_module_identity_fields(tmp_path: Path):
    plugin_id = "demo_meta_identity"
    plugin_dir = tmp_path / plugin_id
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": plugin_id,
                "name": "Metadata Name",
                "version": "1.2.3",
                "author": "Metadata Author",
                "description": "Metadata Description",
                "entry": "__init__.py",
                "role": "adapter",
                "permissions": [],
            }
        ),
        encoding="utf-8",
    )
    (plugin_dir / "__init__.py").write_text("", encoding="utf-8")

    module_name = f"{tmp_path.name}.{plugin_id}"
    mod = types.ModuleType(module_name)

    def setup(plg: Plugin):
        @plg.on_command("meta_wins")
        async def meta_wins(c, args):
            return None

    mod.setup = setup  # type: ignore[attr-defined]
    mod.__plugin_name__ = "Module Name"  # type: ignore[attr-defined]
    mod.__plugin_version__ = "9.9.9"  # type: ignore[attr-defined]
    mod.__plugin_author__ = "Module Author"  # type: ignore[attr-defined]
    mod.__plugin_description__ = "Module Description"  # type: ignore[attr-defined]
    mod.__plugin_role__ = PluginRole.LOGIC  # type: ignore[attr-defined]
    sys.modules[module_name] = mod

    parent = str(tmp_path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    cmd_reg = CommandRegistry()
    event_bus = EventBus()
    mgr = PluginManager(cmd_reg, event_bus, data_dir=tmp_path)

    try:
        await mgr.load_plugins_from_metadata_dir_async(tmp_path)

        loaded_meta = mgr.get_plugin(plugin_id)
        assert loaded_meta is not None
        assert loaded_meta.name == "Metadata Name"
        assert loaded_meta.version == "1.2.3"
        assert loaded_meta.author == "Metadata Author"
        assert loaded_meta.description == "Metadata Description"
        assert loaded_meta.role == PluginRole.ADAPTER

        await mgr.disable_plugin_async(plugin_id)
        enabled_meta = await mgr.enable_plugin_async(plugin_id)
        assert enabled_meta.name == "Metadata Name"
        assert enabled_meta.version == "1.2.3"
        assert enabled_meta.author == "Metadata Author"
        assert enabled_meta.description == "Metadata Description"
        assert enabled_meta.role == PluginRole.ADAPTER

        # Reload should preserve metadata.json identity source as well.
        mod.__plugin_name__ = "Module Renamed"  # type: ignore[attr-defined]
        mod.__plugin_version__ = "8.8.8"  # type: ignore[attr-defined]
        mod.__plugin_author__ = "Another Author"  # type: ignore[attr-defined]
        mod.__plugin_description__ = "Another Description"  # type: ignore[attr-defined]
        mod.__plugin_role__ = PluginRole.LOGIC  # type: ignore[attr-defined]

        reloaded_meta = await mgr.reload_plugin_async(plugin_id)
        assert reloaded_meta.name == "Metadata Name"
        assert reloaded_meta.version == "1.2.3"
        assert reloaded_meta.author == "Metadata Author"
        assert reloaded_meta.description == "Metadata Description"
        assert reloaded_meta.role == PluginRole.ADAPTER
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.asyncio
async def test_boot_applies_persisted_disabled_plugin_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    builtin_root = tmp_path / "empty_builtin_plugins"
    builtin_root.mkdir()
    monkeypatch.setattr("shinbot.core.plugins.manager._BUILTIN_PLUGINS_DIR", builtin_root)

    data_dir = tmp_path / "data"
    plugin_id = "demo_boot_disabled"
    plugin_dir = data_dir / "plugins" / plugin_id
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": plugin_id,
                "name": "Boot Disabled",
                "version": "1.0.0",
                "author": "test",
                "description": "",
                "entry": "__init__.py",
                "permissions": [],
            }
        ),
        encoding="utf-8",
    )
    (plugin_dir / "__init__.py").write_text(
        "\n".join(
            [
                "def setup(plg):",
                '    @plg.on_command("boot_disabled")',
                "    async def boot_disabled(ctx, args):",
                "        return None",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[plugins]]",
                f'id = "{plugin_id}"',
                "enabled = false",
            ]
        ),
        encoding="utf-8",
    )

    boot = BootController(config_path=config_path, data_dir=data_dir)
    try:
        bot = await boot.boot()
        meta = bot.plugin_manager.get_plugin(plugin_id)

        assert meta is not None
        assert meta.state == PluginState.DISABLED
        assert bot.command_registry.get("boot_disabled") is None
    finally:
        await boot.shutdown()
        sys.modules.pop(f"plugins.{plugin_id}", None)
