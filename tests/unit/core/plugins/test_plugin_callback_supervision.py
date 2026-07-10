"""Unit tests for owner-aware plugin callback supervision."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteTargetRegistry
from shinbot.core.message_routes.command import CommandRegistry
from shinbot.core.plugins.context import Plugin
from shinbot.core.tools import ToolRegistry


class _ObserverRegistry:
    """Minimal model-runtime observer registry for plugin tests."""

    def __init__(self) -> None:
        self.observers: list[Callable[..., Any]] = []

    def register_observer(self, observer: Callable[..., Any]) -> None:
        self.observers.append(observer)

    def unregister_observer(self, observer: Callable[..., Any]) -> None:
        self.observers = [item for item in self.observers if item is not observer]


class _CronManager:
    """Capture cron callbacks without starting APScheduler."""

    def __init__(self) -> None:
        self.jobs: dict[str, Callable[..., Any]] = {}

    def add_cron_job(
        self,
        plugin_id: str,
        func: Callable[..., Any],
        cron_expr: str,
        *,
        timezone: str | None = None,
        job_id: str | None = None,
        description: str = "",
    ) -> str:
        del cron_expr, timezone, description
        resolved_job_id = job_id or f"{plugin_id}-job"
        self.jobs[resolved_job_id] = func
        return resolved_job_id


class _InstallerRegistry:
    """Capture marketplace installer callbacks registered by a plugin."""

    def __init__(self) -> None:
        self.installer: dict[str, Any] = {}

    def register_plugin_installer(
        self,
        installer_type: str,
        *,
        owner_plugin_id: str,
        install_fn: Callable[..., Any],
        uninstall_fn: Callable[..., Any] | None,
        validate_fn: Callable[..., Any] | None,
        target_dir: Path | str | None,
    ) -> None:
        self.installer = {
            "type": installer_type,
            "owner": owner_plugin_id,
            "install": install_fn,
            "uninstall": uninstall_fn,
            "validate": validate_fn,
            "target_dir": target_dir,
        }


def _make_plugin(
    targets: RouteTargetRegistry,
    *,
    tool_registry: ToolRegistry | None = None,
    model_runtime: _ObserverRegistry | None = None,
    cron_manager: _CronManager | None = None,
    plugin_manager: _InstallerRegistry | None = None,
) -> Plugin:
    return Plugin(
        "callback-owner",
        CommandRegistry(),
        EventBus(),
        route_targets=targets,
        tool_registry=tool_registry,
        model_runtime=model_runtime,
        cron_manager=cron_manager,
        plugin_manager=plugin_manager,
    )


@pytest.mark.asyncio
async def test_tool_callback_preserves_result_and_is_drained_on_owner_cancel() -> None:
    targets = RouteTargetRegistry()
    registry = ToolRegistry()
    plugin = _make_plugin(targets, tool_registry=registry)
    started = asyncio.Event()
    cancellation_started = asyncio.Event()
    allow_cancellation = asyncio.Event()
    call_count = 0

    @plugin.tool(
        name="blocking_tool",
        description="Block until cancelled",
        input_schema={"type": "object", "properties": {}},
    )
    async def blocking_tool(arguments: dict[str, Any], runtime: object) -> dict[str, Any]:
        nonlocal call_count
        del runtime
        call_count += 1
        if arguments.get("immediate"):
            return {"value": arguments["value"]}
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_started.set()
            await allow_cancellation.wait()
            raise

    definition = registry.get_tool_by_name("blocking_tool")
    assert definition is not None
    assert definition.handler is not blocking_tool
    assert inspect.signature(definition.handler) == inspect.signature(blocking_tool)
    assert await definition.handler({"immediate": True, "value": 7}, object()) == {
        "value": 7
    }

    invocation = asyncio.create_task(definition.handler({}, object()))
    await started.wait()
    assert targets.pending_task_count_for_owner(plugin.plugin_id) == 1

    cancel_task = asyncio.create_task(targets.cancel_owner_tasks(plugin.plugin_id))
    await cancellation_started.wait()
    assert not cancel_task.done()

    allow_cancellation.set()
    await cancel_task
    with pytest.raises(RuntimeError, match="plugin owner is inactive"):
        await invocation

    assert targets.pending_task_count_for_owner(plugin.plugin_id) == 0
    with pytest.raises(RuntimeError, match="plugin owner is inactive"):
        await definition.handler({"immediate": True, "value": 8}, object())
    assert call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_kind", ["observer", "cron"])
async def test_passive_callback_is_cancelled_and_skipped_after_owner_block(
    callback_kind: str,
) -> None:
    targets = RouteTargetRegistry()
    observer_registry = _ObserverRegistry()
    cron_manager = _CronManager()
    plugin = _make_plugin(
        targets,
        model_runtime=observer_registry,
        cron_manager=cron_manager,
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()
    call_count = 0

    async def callback(*args: Any, **kwargs: Any) -> None:
        nonlocal call_count
        del args, kwargs
        call_count += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    if callback_kind == "observer":
        plugin.register_model_runtime_observer(callback)
        registered = observer_registry.observers[0]
        invocation = asyncio.create_task(registered({"event": "test"}))
        assert plugin._registered_model_observers == [registered]
    else:
        plugin.on_cron("* * * * *", job_id="owned-cron")(callback)
        registered = cron_manager.jobs["owned-cron"]
        invocation = asyncio.create_task(registered())

    assert registered is not callback
    assert inspect.signature(registered) == inspect.signature(callback)
    await started.wait()

    await targets.cancel_owner_tasks(plugin.plugin_id)
    assert await invocation is None
    assert cancelled.is_set()
    assert targets.pending_task_count_for_owner(plugin.plugin_id) == 0

    if callback_kind == "observer":
        assert await registered({"event": "late"}) is None
    else:
        assert await registered() is None
    assert call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["install", "uninstall"])
async def test_installer_async_callback_is_drained_and_rejects_new_work(
    operation: str,
) -> None:
    targets = RouteTargetRegistry()
    installers = _InstallerRegistry()
    plugin = _make_plugin(targets, plugin_manager=installers)
    started = asyncio.Event()
    cancelled = asyncio.Event()
    call_count = 0

    async def blocking_callback(plugin_ref: object, **kwargs: Any) -> bool:
        nonlocal call_count
        del plugin_ref, kwargs
        call_count += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def immediate_callback(plugin_ref: object, **kwargs: Any) -> bool:
        del plugin_ref, kwargs
        return True

    plugin.register_plugin_installer(
        "custom",
        blocking_callback if operation == "install" else immediate_callback,
        blocking_callback if operation == "uninstall" else immediate_callback,
    )
    registered = installers.installer[operation]
    assert callable(registered)
    assert inspect.signature(registered) == inspect.signature(blocking_callback)

    invocation = asyncio.create_task(registered("plugin", source_info={"id": "demo"}))
    await started.wait()
    await targets.cancel_owner_tasks(plugin.plugin_id)

    with pytest.raises(RuntimeError, match="plugin owner is inactive"):
        await invocation
    assert cancelled.is_set()
    assert targets.pending_task_count_for_owner(plugin.plugin_id) == 0

    with pytest.raises(RuntimeError, match="plugin owner is inactive"):
        await registered("plugin")
    assert call_count == 1


@pytest.mark.asyncio
async def test_installer_wrappers_preserve_results_and_validator_stays_sync() -> None:
    targets = RouteTargetRegistry()
    installers = _InstallerRegistry()
    plugin = _make_plugin(targets, plugin_manager=installers)
    validate_calls = 0

    async def install(plugin_root: Path, *, source_info: dict[str, Any]) -> dict[str, Any]:
        return {"root": plugin_root.name, "source": source_info["id"]}

    def uninstall(plugin_id: str, *, target_dir: Path | None = None) -> str:
        return f"{plugin_id}:{target_dir}"

    def validate(plugin_root: Path) -> dict[str, Any]:
        nonlocal validate_calls
        validate_calls += 1
        return {"id": plugin_root.name}

    plugin.register_plugin_installer(
        "custom",
        install,
        uninstall,
        validate,
    )
    registered_install = installers.installer["install"]
    registered_uninstall = installers.installer["uninstall"]
    registered_validate = installers.installer["validate"]
    assert callable(registered_install)
    assert callable(registered_uninstall)
    assert callable(registered_validate)
    assert inspect.signature(registered_install) == inspect.signature(install)
    assert inspect.signature(registered_uninstall) == inspect.signature(uninstall)
    assert inspect.signature(registered_validate) == inspect.signature(validate)
    assert not inspect.iscoroutinefunction(registered_validate)

    assert await registered_install(Path("demo"), source_info={"id": "source"}) == {
        "root": "demo",
        "source": "source",
    }
    assert await registered_uninstall("demo", target_dir=None) == "demo:None"
    assert registered_validate(Path("demo")) == {"id": "demo"}

    await targets.cancel_owner_tasks(plugin.plugin_id)
    with pytest.raises(RuntimeError, match="plugin owner is inactive"):
        registered_validate(Path("late"))
    assert validate_calls == 1
