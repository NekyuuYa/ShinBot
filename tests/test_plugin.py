"""Tests for plugin lifecycle and registration."""

import json
import sys
import types
from pathlib import Path

import pytest

from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.plugins.plugin import (
    PluginContext,
    PluginManager,
    PluginState,
    _topo_sort,
)


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


class TestPluginContext:
    def setup_method(self):
        self.cmd_reg = CommandRegistry()
        self.event_bus = EventBus()
        self.ctx = PluginContext("test-plugin", self.cmd_reg, self.event_bus)

    def test_on_command_decorator(self):
        @self.ctx.on_command("hello", aliases=["hi"], permission="cmd.hello")
        async def handler(ctx, args):
            pass

        assert self.cmd_reg.get("hello") is not None
        assert self.cmd_reg.get("hi") is not None
        assert "hello" in self.ctx._registered_commands

    def test_on_event_decorator(self):
        @self.ctx.on_event("message-created")
        async def handler(event):
            pass

        assert self.event_bus.handler_count("message-created") == 1
        assert "message-created" in self.ctx._registered_events

    def test_on_message_shorthand(self):
        @self.ctx.on_message()
        async def handler(event):
            pass

        assert self.event_bus.handler_count("message-created") == 1


class TestPluginManager:
    def setup_method(self):
        self.cmd_reg = CommandRegistry()
        self.event_bus = EventBus()
        self.mgr = PluginManager(self.cmd_reg, self.event_bus)

    def teardown_method(self):
        # Clean up any test modules
        for key in list(sys.modules.keys()):
            if key.startswith("test_plugin_"):
                del sys.modules[key]

    def test_load_plugin(self):
        def setup(ctx: PluginContext):
            @ctx.on_command("greet")
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
        assert meta.name == "Greeter"
        assert "greet" in meta.commands
        assert self.cmd_reg.get("greet") is not None

    def test_load_duplicate_raises(self):
        _make_plugin_module("test_plugin_dup", setup_fn=lambda ctx: None)
        self.mgr.load_plugin("dup", "test_plugin_dup")
        with pytest.raises(ValueError, match="already loaded"):
            self.mgr.load_plugin("dup", "test_plugin_dup")

    def test_load_no_setup_raises(self):
        mod = types.ModuleType("test_plugin_nosetup")
        sys.modules["test_plugin_nosetup"] = mod
        with pytest.raises(AttributeError, match="setup"):
            self.mgr.load_plugin("nosetup", "test_plugin_nosetup")

    def test_unload_plugin(self):
        def setup(ctx: PluginContext):
            @ctx.on_command("bye")
            async def bye(c, args):
                pass

            @ctx.on_event("test-event")
            async def on_test(event):
                pass

        _make_plugin_module("test_plugin_bye", setup_fn=setup)
        self.mgr.load_plugin("bye", "test_plugin_bye")

        assert self.cmd_reg.get("bye") is not None
        assert self.event_bus.handler_count("test-event") == 1

        result = self.mgr.unload_plugin("bye")
        assert result is True
        assert self.cmd_reg.get("bye") is None
        assert self.event_bus.handler_count("test-event") == 0
        assert self.mgr.get_plugin("bye") is None

    def test_unload_nonexistent(self):
        assert self.mgr.unload_plugin("nonexistent") is False

    def test_disable_plugin_keeps_metadata_and_unregisters_handlers(self):
        def setup(ctx: PluginContext):
            @ctx.on_command("sleep")
            async def sleep(c, args):
                pass

            @ctx.on_event("test-event")
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
        def setup(ctx: PluginContext):
            @ctx.on_command("wake")
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

        def setup(ctx: PluginContext):
            call_count["n"] += 1

            @ctx.on_command("reload_test")
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

        def setup(ctx):
            pass

        def teardown():
            torn_down["called"] = True

        _make_plugin_module("test_plugin_teardown", setup_fn=setup, teardown_fn=teardown)
        self.mgr.load_plugin("td", "test_plugin_teardown")
        self.mgr.unload_plugin("td")
        assert torn_down["called"] is True

    def test_all_plugins(self):
        _make_plugin_module("test_plugin_a1", setup_fn=lambda ctx: None)
        _make_plugin_module("test_plugin_b1", setup_fn=lambda ctx: None)
        self.mgr.load_plugin("a", "test_plugin_a1")
        self.mgr.load_plugin("b", "test_plugin_b1")
        assert len(self.mgr.all_plugins) == 2

    def test_get_plugin(self):
        _make_plugin_module("test_plugin_get1", setup_fn=lambda ctx: None)
        self.mgr.load_plugin("get1", "test_plugin_get1")
        assert self.mgr.get_plugin("get1") is not None
        assert self.mgr.get_plugin("nope") is None

    @pytest.mark.asyncio
    async def test_load_plugin_async(self):
        async def async_setup(ctx: PluginContext):
            @ctx.on_command("async_cmd")
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
        def setup(ctx: PluginContext):
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
            json.dumps({
                "id": pid,
                "name": pid,
                "version": "1.0.0",
                "author": "test",
                "description": "",
                "entry": "__init__.py",
                "permissions": [],
                "dependencies": deps,
            })
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
    mgr = PluginManager(cmd_reg, event_bus)
    await mgr.load_plugins_from_metadata_dir_async(tmp_path)

    assert load_order.index("plugin_z") < load_order.index("plugin_b")
    assert "plugin_a" in load_order
