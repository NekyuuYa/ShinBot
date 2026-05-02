"""Tests for plugin lifecycle and registration."""

import json
import sys
import tempfile
import types
from pathlib import Path

import pytest

from shinbot.agent.tools import ToolRegistry
from shinbot.core.application.boot import BootController
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteTable
from shinbot.core.message_routes.command import CommandRegistry
from shinbot.core.message_routes.keyword import KeywordRegistry
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.manager import PluginManager, _topo_sort
from shinbot.core.plugins.types import PluginRole, PluginState


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

    def test_on_message_shorthand_is_removed(self):
        with pytest.raises(ValueError, match="on_message"):
            self.plg.on_message()

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
                f"[plugin_states.{plugin_id}]",
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
