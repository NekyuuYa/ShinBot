"""Plugin system — lifecycle, registration, and decorator API.

Implements the plugin specification (07_plugin_system_design.md).

Two plugin roles:
  - Logic Plugins: business features (weather, translate, etc.)
  - Adapter Plugins: protocol drivers (OneBot, Discord, etc.)

Plugins register commands and event handlers via decorators.
PluginManager handles loading, unloading, and hot-reload with
automatic cleanup of all registered hooks.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.tools import ToolDefinition, ToolOwnerType, ToolRegistry, ToolVisibility
from shinbot.core.dispatch.command import CommandDef, CommandMode, CommandPriority, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.utils.logger import get_logger, get_plugin_logger

if TYPE_CHECKING:
    from shinbot.core.platform.adapter_manager import AdapterManager

logger = get_logger(__name__)

# Naming rules for metadata-directory based plugin loading
_VALID_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_")

# Absolute path to the built-in plugins directory (shinbot/builtin_plugins/)
_BUILTIN_PLUGINS_DIR = Path(__file__).resolve().parents[2] / "builtin_plugins"


def _topo_sort(
    candidates: list[tuple[Path, dict[str, Any]]],
) -> list[tuple[Path, dict[str, Any]]]:
    """Topological sort of plugin candidates by their declared ``dependencies`` list.

    Plugins are sorted so that each plugin's declared dependencies are loaded
    before it. Plugins not in the candidate set are silently skipped in the
    traversal (the caller emits missing-dependency warnings separately).
    Cycles are logged as errors; the participating plugins are appended at the
    end in their original order so loading can still proceed.
    """
    id_to_item: dict[str, tuple[Path, dict[str, Any]]] = {
        m["id"]: (d, m) for d, m in candidates
    }
    visited: set[str] = set()
    in_stack: set[str] = set()
    result: list[tuple[Path, dict[str, Any]]] = []

    def visit(pid: str) -> None:
        if pid in visited or pid not in id_to_item:
            return
        if pid in in_stack:
            logger.error(
                "Plugin dependency cycle detected at %r — placing after all non-cyclic plugins",
                pid,
            )
            return
        in_stack.add(pid)
        _, meta = id_to_item[pid]
        for dep in meta.get("dependencies", []):
            visit(dep)
        in_stack.discard(pid)
        if pid not in visited:
            visited.add(pid)
            result.append(id_to_item[pid])

    for _, meta in candidates:
        visit(meta["id"])
    return result


class PluginRole(Enum):
    LOGIC = "logic"
    ADAPTER = "adapter"


class PluginState(Enum):
    LOADED = "loaded"
    ACTIVE = "active"
    DISABLED = "disabled"
    LOAD_FAILED = "load_failed"
    ERROR = "error"
    UNLOADED = "unloaded"


@dataclass
class PluginMeta:
    """Metadata for a loaded plugin."""

    id: str
    name: str = ""
    version: str = "0.0.0"
    description: str = ""
    author: str = ""
    role: PluginRole = PluginRole.LOGIC
    state: PluginState = PluginState.LOADED
    module_path: str = ""
    commands: list[str] = field(default_factory=list)  # registered command names
    event_types: list[str] = field(default_factory=list)  # subscribed event types
    data_dir: str = ""


class PluginContext:
    """Context object passed to plugins during initialization.

    Provides the API surface for plugins to register commands,
    subscribe to events, and interact with the framework.
    """

    def __init__(
        self,
        plugin_id: str,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
    ):
        self.plugin_id = plugin_id
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self.data_dir = (
            Path(data_dir) if data_dir is not None else Path("data") / "plugin_data" / plugin_id
        )
        self._registered_commands: list[str] = []
        self._registered_events: list[str] = []
        self._registered_tools: list[str] = []
        self.logger = get_plugin_logger(plugin_id)

    def on_command(
        self,
        name: str,
        *,
        aliases: list[str] | None = None,
        description: str = "",
        usage: str = "",
        permission: str = "",
        mode: CommandMode = CommandMode.DELEGATED,
        priority: CommandPriority = CommandPriority.P0_PREFIX,
        pattern: str | None = None,
    ) -> Callable:
        """Decorator to register a command handler.

        Usage:
            @ctx.on_command("weather", aliases=["w"], permission="cmd.weather")
            async def weather_handler(ctx, args):
                ...
        """
        import re as _re

        def decorator(func: Callable) -> Callable:
            compiled_pattern = _re.compile(pattern) if pattern else None
            cmd = CommandDef(
                name=name,
                handler=func,
                mode=mode,
                aliases=aliases or [],
                description=description,
                usage=usage,
                permission=permission,
                priority=priority,
                pattern=compiled_pattern,
                owner=self.plugin_id,
            )
            self._command_registry.register(cmd)
            self._registered_commands.append(name)
            return func

        return decorator

    def on_event(
        self,
        event_type: str,
        *,
        priority: int = 100,
    ) -> Callable:
        """Decorator to subscribe to an event type.

        Usage:
            @ctx.on_event("message-created")
            async def on_message(event):
                ...
        """

        def decorator(func: Callable) -> Callable:
            self._event_bus.on(event_type, func, priority=priority, owner=self.plugin_id)
            self._registered_events.append(event_type)
            return func

        return decorator

    def on_message(self, *, priority: int = 100) -> Callable:
        """Shorthand for @on_event("message-created")."""
        return self.on_event("message-created", priority=priority)

    def register_adapter_factory(self, name: str, factory: Callable) -> None:
        """Register an adapter factory with the AdapterManager.

        Only available to plugins whose PluginManager was constructed with an
        AdapterManager reference (i.e. adapter plugins loaded via ShinBot).

        Args:
            name: Platform identifier (e.g. "satori", "onebot_v11").
            factory: Callable ``(instance_id, platform, **kwargs) -> BaseAdapter``.

        Raises:
            RuntimeError: If no AdapterManager is injected in this context.
        """
        if self._adapter_manager is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register an adapter factory: "
                "no AdapterManager is available in this PluginContext."
            )
        self._adapter_manager.register_adapter(name, factory)

    def tool(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        display_name: str = "",
        output_schema: dict[str, Any] | None = None,
        permission: str = "",
        enabled: bool = True,
        visibility: ToolVisibility = ToolVisibility.SCOPED,
        timeout_seconds: float = 30.0,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Callable:
        if self._tool_registry is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register tools: no ToolRegistry is available."
            )

        def decorator(func: Callable) -> Callable:
            tool_id = f"{self.plugin_id}.{name}"
            definition = ToolDefinition(
                id=tool_id,
                name=name,
                display_name=display_name or name,
                description=description,
                input_schema=input_schema,
                output_schema=output_schema,
                handler=func,
                owner_type=ToolOwnerType.PLUGIN,
                owner_id=self.plugin_id,
                owner_module=getattr(func, "__module__", ""),
                permission=permission,
                enabled=enabled,
                visibility=visibility,
                timeout_seconds=timeout_seconds,
                tags=list(tags or []),
                metadata=dict(metadata or {}),
            )
            self._tool_registry.register_tool(definition)
            self._registered_tools.append(tool_id)
            return func

        return decorator


class PluginManager:
    """Manages plugin lifecycle: load, unload, reload.

    Plugins are Python modules that expose a `setup(ctx: PluginContext)` function.
    On load, the module is imported and setup() is called with a PluginContext.
    On unload, all registered commands and event handlers are cleaned up.
    """

    def __init__(
        self,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
    ):
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._plugins: dict[str, PluginMeta] = {}
        self._contexts: dict[str, PluginContext] = {}
        self._modules: dict[str, Any] = {}

        self._root_data_dir = Path(data_dir) if data_dir is not None else Path("data")
        self._plugin_data_root = self._root_data_dir / "plugin_data"
        self._plugin_data_root.mkdir(parents=True, exist_ok=True)

    def _build_ctx(self, plugin_id: str) -> PluginContext:
        """Create a PluginContext for the given plugin_id, injecting all dependencies."""
        return PluginContext(
            plugin_id,
            self._command_registry,
            self._event_bus,
            data_dir=self._build_plugin_data_dir(plugin_id),
            adapter_manager=self._adapter_manager,
            tool_registry=self._tool_registry,
        )

    @property
    def all_plugins(self) -> list[PluginMeta]:
        return list(self._plugins.values())

    def get_plugin(self, plugin_id: str) -> PluginMeta | None:
        return self._plugins.get(plugin_id)

    def load_plugin(self, plugin_id: str, module_path: str) -> PluginMeta:
        """Sync wrapper for plugin loading."""
        return self._run_sync(self.load_plugin_async(plugin_id, module_path))

    async def load_plugin_async(self, plugin_id: str, module_path: str) -> PluginMeta:
        """Load a plugin that has an async setup function."""
        if plugin_id in self._plugins:
            raise ValueError(f"Plugin {plugin_id!r} is already loaded")

        try:
            module = importlib.import_module(module_path)
        except Exception:
            meta = PluginMeta(
                id=plugin_id,
                module_path=module_path,
                state=PluginState.LOAD_FAILED,
            )
            self._plugins[plugin_id] = meta
            logger.exception("Failed to import plugin %s from %s", plugin_id, module_path)
            raise

        if not hasattr(module, "setup"):
            raise AttributeError(f"Plugin module {module_path!r} must expose a setup(ctx) function")

        ctx = self._build_ctx(plugin_id)

        try:
            await self._invoke(module.setup, ctx)
            await self._invoke_hook(module, "on_enable", ctx)
        except Exception:
            logger.exception("Error loading plugin %s", plugin_id)
            self._command_registry.unregister_by_owner(plugin_id)
            self._event_bus.off_all(plugin_id)
            if self._tool_registry is not None:
                self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
            raise

        meta = PluginMeta(
            id=plugin_id,
            name=getattr(module, "__plugin_name__", plugin_id),
            version=getattr(module, "__plugin_version__", "0.0.0"),
            description=getattr(module, "__plugin_description__", ""),
            author=getattr(module, "__plugin_author__", ""),
            role=getattr(module, "__plugin_role__", PluginRole.LOGIC),
            state=PluginState.ACTIVE,
            module_path=module_path,
            commands=list(ctx._registered_commands),
            event_types=list(ctx._registered_events),
            data_dir=str(ctx.data_dir),
        )

        self._plugins[plugin_id] = meta
        self._contexts[plugin_id] = ctx
        self._modules[plugin_id] = module

        logger.info("Loaded plugin %s (async, data_dir=%s)", plugin_id, meta.data_dir)
        return meta

    def unload_plugin(self, plugin_id: str) -> bool:
        """Sync wrapper for plugin unloading."""
        return self._run_sync(self.unload_plugin_async(plugin_id))

    async def unload_plugin_async(self, plugin_id: str, *, remove_module: bool = True) -> bool:
        """Unload a plugin, cleaning up all its registrations."""
        meta = self._plugins.pop(plugin_id, None)
        if meta is None:
            return False
        cmd_count, evt_count = await self._deactivate_plugin_runtime(
            plugin_id,
            meta,
            remove_module=remove_module,
        )
        logger.info("Unloaded plugin %s (removed %d commands, %d event handlers)", plugin_id, cmd_count, evt_count)
        return True

    async def unload_all_plugins_async(self) -> None:
        for plugin_id in list(self._plugins.keys()):
            await self.unload_plugin_async(plugin_id)

    def disable_plugin(self, plugin_id: str) -> PluginMeta:
        return self._run_sync(self.disable_plugin_async(plugin_id))

    async def disable_plugin_async(self, plugin_id: str) -> PluginMeta:
        meta = self._plugins.get(plugin_id)
        if meta is None:
            raise ValueError(f"Plugin {plugin_id!r} is not loaded")
        if meta.state == PluginState.DISABLED:
            return meta

        cmd_count, evt_count = await self._deactivate_plugin_runtime(
            plugin_id,
            meta,
            remove_module=False,
        )
        meta.state = PluginState.DISABLED
        logger.info("Disabled plugin %s (removed %d commands, %d event handlers)", plugin_id, cmd_count, evt_count)
        return meta

    def enable_plugin(self, plugin_id: str) -> PluginMeta:
        return self._run_sync(self.enable_plugin_async(plugin_id))

    async def enable_plugin_async(self, plugin_id: str) -> PluginMeta:
        meta = self._plugins.get(plugin_id)
        if meta is None:
            raise ValueError(f"Plugin {plugin_id!r} is not loaded")
        if meta.state == PluginState.ACTIVE:
            return meta

        module_path = meta.module_path
        existing = sys.modules.get(module_path)
        if existing is not None and getattr(existing, "__spec__", None) is not None:
            module = importlib.reload(existing)
        elif existing is not None:
            module = existing
        else:
            module = importlib.import_module(module_path)

        if not hasattr(module, "setup"):
            raise AttributeError(f"Plugin module {module_path!r} must expose a setup(ctx) function")

        ctx = self._build_ctx(plugin_id)
        await self._invoke(module.setup, ctx)
        await self._invoke_hook(module, "on_enable", ctx)

        meta.name = getattr(module, "__plugin_name__", plugin_id)
        meta.version = getattr(module, "__plugin_version__", "0.0.0")
        meta.description = getattr(module, "__plugin_description__", "")
        meta.author = getattr(module, "__plugin_author__", "")
        meta.role = getattr(module, "__plugin_role__", PluginRole.LOGIC)
        meta.state = PluginState.ACTIVE
        meta.commands = list(ctx._registered_commands)
        meta.event_types = list(ctx._registered_events)
        meta.data_dir = str(ctx.data_dir)
        self._contexts[plugin_id] = ctx
        self._modules[plugin_id] = module
        logger.info("Enabled plugin %s", plugin_id)
        return meta

    def load_plugins_from_dir(self, directory: Path | str, *, prefix: str = "") -> list[PluginMeta]:
        """Scan a directory and load all Python plugin modules found.

        Discovery rules:
          - `{directory}/{name}.py`  → module "{pkg}.{name}"  (single file)
          - `{directory}/{name}/__init__.py` → module "{pkg}.{name}"  (package)

        Args:
            directory: Path to the plugins directory.
            prefix: Optional module path prefix. If empty, the directory's
                    parent is added to sys.path and the directory name is used
                    as the top-level package name.

        Returns:
            List of successfully loaded PluginMeta.
        """
        directory = Path(directory)
        if not directory.is_dir():
            raise NotADirectoryError(f"Plugin directory not found: {directory}")

        if not prefix:
            parent = str(directory.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)
            prefix = directory.name

        loaded: list[PluginMeta] = []

        for entry in sorted(directory.iterdir()):
            if entry.name.startswith("_"):
                continue  # skip __init__, __pycache__, etc.

            if entry.is_file() and entry.suffix == ".py":
                module_path = f"{prefix}.{entry.stem}"
                plugin_id = entry.stem
            elif entry.is_dir() and (entry / "__init__.py").exists():
                module_path = f"{prefix}.{entry.name}"
                plugin_id = entry.name
            else:
                continue

            if plugin_id in self._plugins:
                logger.debug("Plugin %r already loaded, skipping", plugin_id)
                continue

            try:
                meta = self.load_plugin(plugin_id, module_path)
                loaded.append(meta)
            except Exception:
                logger.exception("Failed to load plugin %r from %s", plugin_id, module_path)

        return loaded

    def load_plugins_from_metadata_dir(self, directory: Path | str) -> list[PluginMeta]:
        return self._run_sync(self.load_plugins_from_metadata_dir_async(directory))

    async def load_plugins_from_metadata_dir_async(self, directory: Path | str) -> list[PluginMeta]:
        """Load user plugins from a metadata directory (e.g. ``data/plugins/``).

        Naming-prefix rules are **not** enforced here so that third-party
        plugin authors can freely choose names.  For strict enforcement
        (including built-in plugins) use ``load_all_async``.
        """
        return await self._load_from_metadata_dir_async(Path(directory), is_builtin=False)

    async def load_all_async(self, user_dir: Path | str | None = None) -> list[PluginMeta]:
        """Scan and load both built-in and user plugin directories.

        Loading order:
          1. ``shinbot/builtin_plugins/``  — strict naming, shipped with the package.
          2. ``{user_dir}``  (default: ``data/plugins/``) — user-installed plugins.

        Returns:
            Flat list of all newly loaded PluginMeta.
        """
        results: list[PluginMeta] = []

        if _BUILTIN_PLUGINS_DIR.is_dir():
            results.extend(
                await self._load_from_metadata_dir_async(_BUILTIN_PLUGINS_DIR, is_builtin=True)
            )
        else:
            logger.debug("No built-in plugins directory at %s", _BUILTIN_PLUGINS_DIR)

        user_path = Path(user_dir) if user_dir is not None else self._root_data_dir / "plugins"
        if user_path.is_dir():
            results.extend(await self._load_from_metadata_dir_async(user_path, is_builtin=False))

        return results

    async def _load_from_metadata_dir_async(
        self, directory: Path, *, is_builtin: bool
    ) -> list[PluginMeta]:
        """Internal scanner shared by ``load_all_async`` and the public wrapper."""
        if not directory.is_dir():
            raise NotADirectoryError(f"Plugin directory not found: {directory}")

        if not is_builtin:
            parent = str(directory.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)

        # Pass 1: collect and validate metadata for all plugin directories.
        candidates: list[tuple[Path, dict[str, Any]]] = []
        for plugin_dir in sorted(directory.iterdir()):
            if not plugin_dir.is_dir() or plugin_dir.name.startswith("_"):
                continue

            metadata_path = plugin_dir / "metadata.json"
            if not metadata_path.exists():
                logger.debug("No metadata.json in %s, skipping", plugin_dir)
                continue

            try:
                metadata = self._validate_metadata(
                    metadata_path, plugin_dir, require_naming=is_builtin
                )
            except Exception:
                logger.exception("Invalid metadata.json in %s", plugin_dir)
                continue

            if metadata["id"] in self._plugins:
                logger.debug("Plugin %r already loaded, skipping", metadata["id"])
                continue

            candidates.append((plugin_dir, metadata))

        # Pass 2: sort candidates in dependency order so that a declared
        # dependency is always initialized before its dependents.
        sorted_candidates = _topo_sort(candidates)

        # Pass 3: warn on declared dependencies that are not present in this
        # scan batch (they may be separately loaded user plugins).
        batch_ids = {m["id"] for _, m in sorted_candidates}
        already_loaded = set(self._plugins.keys())
        for _, metadata in sorted_candidates:
            for dep in metadata.get("dependencies", []):
                if dep not in batch_ids and dep not in already_loaded:
                    logger.warning(
                        "Plugin %r declares dependency on %r which is not available",
                        metadata["id"],
                        dep,
                    )

        # Pass 4: load in dependency-sorted order.
        loaded: list[PluginMeta] = []
        for plugin_dir, metadata in sorted_candidates:
            plugin_id = metadata["id"]
            entry_file = metadata["entry"]
            permissions = metadata.get("permissions", [])

            if is_builtin:
                pkg = f"shinbot.builtin_plugins.{plugin_dir.name}"
                module_path = (
                    pkg
                    if entry_file == "__init__.py"
                    else (f"{pkg}.{'.'.join(Path(entry_file).with_suffix('').parts)}")
                )
            else:
                prefix = directory.name
                module_path = (
                    f"{prefix}.{plugin_dir.name}"
                    if entry_file == "__init__.py"
                    else f"{prefix}.{plugin_dir.name}.{'.'.join(Path(entry_file).with_suffix('').parts)}"
                )

            try:
                meta = await self.load_plugin_async(plugin_id, module_path)
                self._validate_permissions(plugin_id, permissions, meta)
                loaded.append(meta)
                logger.info(
                    "Loaded %s plugin %s (module=%s)",
                    "builtin" if is_builtin else "user",
                    plugin_id,
                    module_path,
                )
            except Exception:
                logger.exception("Failed to load plugin %s from %s", plugin_id, plugin_dir)

        return loaded

    def _validate_permissions(
        self, plugin_id: str, declared_permissions: list[str], meta: PluginMeta
    ) -> None:
        """Cross-validate that plugin's decorator permissions match metadata.

        This ensures that the permissions declared in metadata.json match
        what the plugin actually registers via @ctx.on_command(permission=...).

        Args:
            plugin_id: The plugin ID.
            declared_permissions: Permissions from metadata.json.
            meta: The loaded plugin metadata.
        """
        # Get registered commands with permission requirements
        cmd_registry = self._command_registry
        registered_commands = cmd_registry._commands.values()

        plugin_perms_used = set()
        for cmd in registered_commands:
            if cmd.owner == plugin_id and cmd.permission:
                plugin_perms_used.add(cmd.permission)

        # Check: all used permissions should be declared in metadata
        undeclared = plugin_perms_used - set(declared_permissions)
        if undeclared:
            logger.warning(
                "Plugin %s uses permissions not declared in metadata: %s",
                plugin_id,
                undeclared,
            )

        # Check: all declared permissions should be reasonable (basic format check)
        for perm in declared_permissions:
            if not perm or "." not in perm:
                logger.debug(
                    "Plugin %s declares unusual permission format: %r",
                    plugin_id,
                    perm,
                )

    def reload_plugin(self, plugin_id: str) -> PluginMeta:
        return self._run_sync(self.reload_plugin_async(plugin_id))

    async def reload_plugin_async(self, plugin_id: str) -> PluginMeta:
        meta = self._plugins.get(plugin_id)
        if meta is None:
            raise ValueError(f"Plugin {plugin_id!r} is not loaded")

        module_path = meta.module_path
        await self.unload_plugin_async(plugin_id, remove_module=False)

        existing = sys.modules.get(module_path)
        if existing is not None and getattr(existing, "__spec__", None) is not None:
            module = importlib.reload(existing)
        elif existing is not None:
            module = existing
        else:
            module = importlib.import_module(module_path)

        logger.info("Reloading plugin %s", plugin_id)

        ctx = self._build_ctx(plugin_id)
        await self._invoke(module.setup, ctx)
        await self._invoke_hook(module, "on_enable", ctx)

        new_meta = PluginMeta(
            id=plugin_id,
            name=getattr(module, "__plugin_name__", plugin_id),
            version=getattr(module, "__plugin_version__", "0.0.0"),
            description=getattr(module, "__plugin_description__", ""),
            author=getattr(module, "__plugin_author__", ""),
            role=getattr(module, "__plugin_role__", PluginRole.LOGIC),
            state=PluginState.ACTIVE,
            module_path=module_path,
            commands=list(ctx._registered_commands),
            event_types=list(ctx._registered_events),
            data_dir=str(ctx.data_dir),
        )
        self._plugins[plugin_id] = new_meta
        self._contexts[plugin_id] = ctx
        self._modules[plugin_id] = module
        return new_meta

    async def _deactivate_plugin_runtime(
        self,
        plugin_id: str,
        meta: PluginMeta,
        *,
        remove_module: bool,
    ) -> tuple[int, int]:
        module = self._modules.get(plugin_id)
        ctx = self._contexts.get(plugin_id)

        try:
            await self._invoke_hook(module, "on_disable", ctx)
        except Exception:
            logger.exception("Error in on_disable() for plugin %s", plugin_id)

        cmd_count = self._command_registry.unregister_by_owner(plugin_id)
        evt_count = self._event_bus.off_all(plugin_id)
        if self._tool_registry is not None:
            self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)

        if module and hasattr(module, "teardown"):
            try:
                await self._invoke(module.teardown)
            except Exception:
                logger.exception("Error in teardown() for plugin %s", plugin_id)

        self._modules.pop(plugin_id, None)
        self._contexts.pop(plugin_id, None)

        if remove_module and meta.module_path in sys.modules:
            del sys.modules[meta.module_path]

        return cmd_count, evt_count

    def _build_plugin_data_dir(self, plugin_id: str) -> Path:
        candidate = (self._plugin_data_root / plugin_id).resolve()
        root = self._plugin_data_root.resolve()
        if root not in candidate.parents and candidate != root:
            raise ValueError(f"Invalid plugin id for data dir: {plugin_id!r}")
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate

    def _validate_metadata(
        self,
        metadata_path: Path,
        plugin_dir: Path,
        *,
        require_naming: bool = False,
    ) -> dict[str, Any]:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not isinstance(metadata, dict):
            raise ValueError("metadata.json must contain a JSON object")

        plugin_id = metadata.get("id")
        entry = metadata.get("entry")

        if not isinstance(plugin_id, str) or not plugin_id.strip():
            raise ValueError("metadata.id must be a non-empty string")
        if not isinstance(entry, str) or not entry.strip():
            raise ValueError("metadata.entry must be a non-empty string")

        plugin_id = plugin_id.strip()

        if require_naming:
            # Folder name must exactly match the plugin ID.
            if plugin_dir.name != plugin_id:
                raise ValueError(
                    f"Plugin folder name {plugin_dir.name!r} must match metadata.id {plugin_id!r}"
                )
            # Plugin ID must follow the mandatory prefix convention.
            if not any(plugin_id.startswith(p) for p in _VALID_PREFIXES):
                raise ValueError(
                    f"Plugin id {plugin_id!r} must start with one of: "
                    + ", ".join(repr(p) for p in _VALID_PREFIXES)
                )

        entry_path = Path(entry)
        if entry_path.is_absolute() or ".." in entry_path.parts:
            raise ValueError("metadata.entry must be a relative path inside plugin directory")

        abs_entry = (plugin_dir / entry_path).resolve()
        plugin_root = plugin_dir.resolve()
        if plugin_root not in abs_entry.parents and abs_entry != plugin_root:
            raise ValueError("metadata.entry must resolve inside plugin directory")
        if not abs_entry.exists() or not abs_entry.is_file():
            raise ValueError(f"metadata.entry file does not exist: {entry}")

        metadata["id"] = plugin_id
        metadata["entry"] = entry_path.as_posix()

        deps = metadata.get("dependencies", [])
        if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
            raise ValueError("metadata.dependencies must be a list of plugin ID strings")
        metadata["dependencies"] = deps

        return metadata

    async def _invoke_hook(self, module: Any, hook_name: str, ctx: PluginContext | None) -> None:
        if module is None or not hasattr(module, hook_name):
            return
        hook = getattr(module, hook_name)
        try:
            sig = inspect.signature(hook)
            if len(sig.parameters) == 0:
                await self._invoke(hook)
            else:
                await self._invoke(hook, ctx)
        except (TypeError, ValueError):
            await self._invoke(hook, ctx)

    async def _invoke(self, func: Callable[..., Any], *args: Any) -> Any:
        result = func(*args)
        if inspect.isawaitable(result):
            return await result
        return result

    def _run_sync(self, awaitable: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(awaitable)
        raise RuntimeError("Cannot call sync plugin API inside a running event loop")
