"""Plugin lifecycle, discovery, and metadata validation."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.core.config_provider import (
    ConfigProviderLoadError,
    ConfigProviderRegistry,
    load_provider_schema_from_module,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteTable
from shinbot.core.message_routes import CommandRegistry, KeywordRegistry
from shinbot.core.model_runtime import ModelRuntimeExtensionRegistrar, ModelRuntimeObserverRegistry
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.dependencies import (
    PluginDependencyError,
    sync_plugin_python_dependencies,
)
from shinbot.core.plugins.types import PluginMeta, PluginRole, PluginState
from shinbot.core.tools import ToolOwnerType, ToolRegistry
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.core.plugins.cron_manager import PluginCronManager

logger = get_logger(__name__, source="plugins", color="yellow")

_VALID_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_")
_BUILTIN_PLUGINS_DIR = Path(__file__).resolve().parents[2] / "builtin_plugins"


def _ensure_user_plugin_package_on_path(directory: Path) -> None:
    """Ensure the user-plugin package root can be imported from ``directory``."""

    parent = str(directory.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    package_name = directory.name
    package_dir = str(directory)
    existing = sys.modules.get(package_name)
    if existing is not None:
        package_paths = getattr(existing, "__path__", None)
        if package_paths is None:
            sys.modules.pop(package_name, None)
        elif package_dir not in package_paths:
            package_paths.append(package_dir)

    importlib.invalidate_caches()


def _ensure_user_plugin_dir_on_path(plugin_dir: Path) -> None:
    """Ensure a package nested under a plugin directory can be imported by name."""

    path = str(plugin_dir)
    if path not in sys.path:
        sys.path.insert(0, path)
    importlib.invalidate_caches()


@dataclass(slots=True, frozen=True)
class PluginDiscoveryCandidate:
    """Resolved metadata for one discovered plugin candidate."""

    plugin_dir: Path
    metadata: dict[str, Any]
    module_path: str
    is_builtin: bool


def _topo_sort(
    candidates: list[tuple[Path, dict[str, Any]]],
) -> list[tuple[Path, dict[str, Any]]]:
    id_to_item: dict[str, tuple[Path, dict[str, Any]]] = {m["id"]: (d, m) for d, m in candidates}
    visited: set[str] = set()
    in_stack: set[str] = set()
    result: list[tuple[Path, dict[str, Any]]] = []

    def visit(pid: str) -> None:
        """Recursively visit a plugin and its dependencies for topological sort."""
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
        dependency_ids = [
            *meta.get("required_dependencies", []),
            *meta.get("optional_dependencies", []),
            *meta.get("dependencies", []),
        ]
        for dep in dependency_ids:
            visit(dep)
        in_stack.discard(pid)
        if pid not in visited:
            visited.add(pid)
            result.append(id_to_item[pid])

    for _, meta in candidates:
        visit(meta["id"])
    return result


class PluginManager:
    """Manages plugin lifecycle: load, unload, reload.

    Provides both synchronous and asynchronous APIs for discovering,
    loading, unloading, enabling, disabling, and reloading plugins.
    Each loaded plugin is backed by a :class:`Plugin` context object
    that is passed to its ``setup(plg)`` entry-point, giving the
    plugin access to commands, events, keywords, routes, tools, and
    configuration.
    """

    def __init__(
        self,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        keyword_registry: KeywordRegistry | None = None,
        route_table: RouteTable | None = None,
        route_targets: RouteTargetRegistry | None = None,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
        model_runtime: ModelRuntimeObserverRegistry | None = None,
        agent_runtime: Any | None = None,
        database: Any | None = None,
        cron_manager: PluginCronManager | None = None,
        config_provider_registry: ConfigProviderRegistry | None = None,
    ) -> None:
        """Initialize the plugin manager.

        Args:
            command_registry: Registry for text command handlers.
            event_bus: Event bus for framework and lifecycle signals.
            data_dir: Root data directory. Plugin-specific data is stored
                under ``<data_dir>/plugin_data/<plugin_id>``.
            keyword_registry: Registry for keyword-matching handlers.
                Creates a new empty registry if *None*.
            route_table: Custom route table for plugin-defined routes.
            route_targets: Registry of route targets for dispatch.
            adapter_manager: Manages platform adapter instances.
            tool_registry: Registry of agent tools. Tools registered by
                plugins are tracked here with ownership metadata.
            model_runtime: Registry for model runtime observers.
            agent_runtime: Runtime object exposed to the agent system.
            database: Shared database handle passed to plugins.
            cron_manager: Plugin cron scheduler for timed tasks.
            config_provider_registry: Registry of plugin configuration
                providers. Creates a new empty registry if *None*.
        """
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._keyword_registry = keyword_registry or KeywordRegistry()
        self._route_table = route_table
        self._route_targets = route_targets
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._model_runtime = model_runtime
        self._agent_runtime = agent_runtime
        self._database = database
        self._cron_manager = cron_manager
        self.config_provider_registry = config_provider_registry or ConfigProviderRegistry()
        self._plugins: dict[str, PluginMeta] = {}
        self._plugin_objects: dict[str, Plugin] = {}
        self._modules: dict[str, Any] = {}
        self._declared_metadata: dict[str, dict[str, Any]] = {}
        self._pre_registered_runtime_plugins: set[str] = set()
        self._boot: Any | None = None  # Set by BootController after creation

        self._root_data_dir = Path(data_dir) if data_dir is not None else Path("data")
        self._plugin_data_root = self._root_data_dir / "plugin_data"
        self._plugin_data_root.mkdir(parents=True, exist_ok=True)

    def attach_runtime_services(
        self,
        *,
        tool_registry: ToolRegistry | None = None,
        model_runtime: ModelRuntimeObserverRegistry | None = None,
        agent_runtime: Any | None = None,
        cron_manager: PluginCronManager | None = None,
    ) -> None:
        """Attach optional runtime capabilities for subsequently built Plugin objects."""
        if tool_registry is not None:
            self._tool_registry = tool_registry
        if model_runtime is not None:
            self._model_runtime = model_runtime
        if agent_runtime is not None:
            self._agent_runtime = agent_runtime
        if cron_manager is not None:
            self._cron_manager = cron_manager
        for plugin in self._plugin_objects.values():
            if tool_registry is not None:
                plugin._tool_registry = tool_registry
            if model_runtime is not None:
                plugin._model_runtime = model_runtime
            if agent_runtime is not None:
                plugin.agent_runtime = agent_runtime
            if cron_manager is not None:
                plugin._cron_manager = cron_manager

    async def preregister_model_runtime_extensions(
        self,
        user_dir: Path | str | None = None,
    ) -> None:
        """Import plugin modules and let them register model-runtime extensions early."""

        registrar = ModelRuntimeExtensionRegistrar()
        for candidate in self._discover_plugin_candidates(user_dir=user_dir):
            plugin_id = candidate.metadata["id"]
            if plugin_id in self._pre_registered_runtime_plugins:
                continue
            try:
                module = importlib.import_module(candidate.module_path)
            except Exception:
                logger.exception(
                    "Failed importing plugin %s for model runtime preregistration",
                    plugin_id,
                )
                continue
            try:
                preregister = getattr(module, "register_model_runtime_extensions", None)
                if preregister is None:
                    continue
                result = preregister(registrar)
                if inspect.isawaitable(result):
                    await result
                self._pre_registered_runtime_plugins.add(plugin_id)
                logger.info(
                    "Pre-registered model runtime extensions from plugin %s",
                    plugin_id,
                )
            except Exception:
                logger.exception(
                    "Failed preregistering model runtime extensions for plugin %s",
                    plugin_id,
                )

    def _build_plg(self, plugin_id: str) -> Plugin:
        return Plugin(
            plugin_id,
            self._command_registry,
            self._event_bus,
            data_dir=self._build_plugin_data_dir(plugin_id),
            keyword_registry=self._keyword_registry,
            route_table=self._route_table,
            route_targets=self._route_targets,
            adapter_manager=self._adapter_manager,
            tool_registry=self._tool_registry,
            model_runtime=self._model_runtime,
            agent_runtime=self._agent_runtime,
            database=self._database,
            cron_manager=self._cron_manager,
            plugin_manager=self,
        )

    @property
    def all_plugins(self) -> list[PluginMeta]:
        """Return a snapshot list of all currently loaded plugin metadata."""
        return list(self._plugins.values())

    def get_plugin(self, plugin_id: str) -> PluginMeta | None:
        """Look up a loaded plugin by its ID.

        Args:
            plugin_id: Unique identifier of the plugin.

        Returns:
            The plugin's metadata, or *None* if no plugin with that ID
            is currently loaded.
        """
        return self._plugins.get(plugin_id)

    def load_plugin(self, plugin_id: str, module_path: str) -> PluginMeta:
        """Synchronously load a single plugin.

        This is a blocking wrapper around :meth:`load_plugin_async`.

        Args:
            plugin_id: Unique identifier assigned to the plugin.
            module_path: Dotted Python module path (e.g.
                ``"shinbot_plugin_foo"``).

        Returns:
            Metadata describing the loaded plugin.

        Raises:
            ValueError: If a plugin with *plugin_id* is already loaded.
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.load_plugin_async(plugin_id, module_path))

    async def load_plugin_async(
        self,
        plugin_id: str,
        module_path: str,
        *,
        declared_metadata: dict[str, Any] | None = None,
    ) -> PluginMeta:
        """Asynchronously load a single plugin.

        Imports the module, calls its ``setup(plg)`` function, then
        registers any declared configuration provider. On failure the
        module is marked :attr:`PluginState.LOAD_FAILED` and all
        handlers registered so far are cleaned up.

        Args:
            plugin_id: Unique identifier assigned to the plugin.
            module_path: Dotted Python module path.
            declared_metadata: Optional pre-parsed metadata dict (e.g.
                from a ``metadata.json``). When provided, identity
                fields are resolved from this dict.

        Returns:
            Metadata describing the loaded plugin.

        Raises:
            ValueError: If a plugin with *plugin_id* is already loaded.
            AttributeError: If the module lacks a ``setup`` function.
            Exception: Re-raises any error from module import or setup,
                after cleaning up partial registrations.
        """
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
            raise AttributeError(f"Plugin module {module_path!r} must expose a setup(plg) function")

        self._register_config_provider_from_module(plugin_id, module)
        plg = self._build_plg(plugin_id)

        try:
            await self._invoke(module.setup, plg)
            await self._invoke_hook(module, "on_enable", plg)
        except Exception:
            logger.exception("Error loading plugin %s", plugin_id)
            self._command_registry.unregister_by_owner(plugin_id)
            self._event_bus.off_all(plugin_id)
            self._keyword_registry.unregister_by_owner(plugin_id)
            self._unregister_routes_by_owner(plugin_id)
            if self._tool_registry is not None:
                self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
            raise

        meta = self._build_plugin_meta(
            plugin_id,
            module_path,
            module,
            plg,
            declared_metadata=declared_metadata,
        )

        self._plugins[plugin_id] = meta
        self._plugin_objects[plugin_id] = plg
        self._modules[plugin_id] = module
        if declared_metadata is not None:
            self._declared_metadata[plugin_id] = dict(declared_metadata)

        logger.info("Loaded plugin %s (async, data_dir=%s)", plugin_id, meta.data_dir)
        return meta

    def unload_plugin(self, plugin_id: str) -> bool:
        """Synchronously unload a plugin.

        This is a blocking wrapper around :meth:`unload_plugin_async`.

        Args:
            plugin_id: ID of the plugin to unload.

        Returns:
            *True* if the plugin was found and unloaded, *False* if it
            was not loaded.

        Raises:
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.unload_plugin_async(plugin_id))

    async def unload_plugin_async(
        self,
        plugin_id: str,
        *,
        remove_module: bool = True,
        remove_declared_metadata: bool = True,
    ) -> bool:
        """Asynchronously unload a plugin.

        Calls the plugin's ``on_disable`` and ``teardown`` hooks, then
        removes all handlers (commands, events, keywords, routes, tools)
        registered by the plugin.

        Args:
            plugin_id: ID of the plugin to unload.
            remove_module: When *True*, removes the module from
                ``sys.modules`` so re-importing picks up changes.
            remove_declared_metadata: When *True*, discards the
                stored declared metadata for the plugin.

        Returns:
            *True* if the plugin was found and unloaded, *False* if it
            was not loaded.
        """
        meta = self._plugins.pop(plugin_id, None)
        if meta is None:
            return False
        cmd_count, evt_count = await self._deactivate_plugin_runtime(
            plugin_id,
            meta,
            remove_module=remove_module,
        )
        logger.info(
            "Unloaded plugin %s (removed %d commands, %d event handlers)",
            plugin_id,
            cmd_count,
            evt_count,
        )
        if remove_declared_metadata:
            self._declared_metadata.pop(plugin_id, None)
        return True

    async def unload_all_plugins_async(self) -> None:
        """Unload every currently loaded plugin."""
        for plugin_id in list(self._plugins.keys()):
            await self.unload_plugin_async(plugin_id)

    def disable_plugin(self, plugin_id: str) -> PluginMeta:
        """Synchronously disable a plugin without fully unloading it.

        This is a blocking wrapper around :meth:`disable_plugin_async`.

        Args:
            plugin_id: ID of the plugin to disable.

        Returns:
            The plugin's metadata with state set to
            :attr:`PluginState.DISABLED`.

        Raises:
            ValueError: If the plugin is not loaded.
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.disable_plugin_async(plugin_id))

    async def disable_plugin_async(self, plugin_id: str) -> PluginMeta:
        """Asynchronously disable a plugin.

        Deactivates all runtime registrations (commands, events, etc.)
        but retains the module reference so the plugin can be
        re-enabled later without a full re-import.

        Args:
            plugin_id: ID of the plugin to disable.

        Returns:
            The plugin's metadata with state set to
            :attr:`PluginState.DISABLED`.

        Raises:
            ValueError: If the plugin is not loaded.
        """
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
        logger.info(
            "Disabled plugin %s (removed %d commands, %d event handlers)",
            plugin_id,
            cmd_count,
            evt_count,
        )
        return meta

    def enable_plugin(self, plugin_id: str) -> PluginMeta:
        """Synchronously re-enable a previously disabled plugin.

        This is a blocking wrapper around :meth:`enable_plugin_async`.

        Args:
            plugin_id: ID of the plugin to enable.

        Returns:
            The plugin's metadata with state set to
            :attr:`PluginState.ACTIVE`.

        Raises:
            ValueError: If the plugin is not loaded.
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.enable_plugin_async(plugin_id))

    async def enable_plugin_async(self, plugin_id: str) -> PluginMeta:
        """Asynchronously re-enable a previously disabled plugin.

        Re-imports (or reloads) the module, calls ``setup(plg)`` and
        ``on_enable`` again. If setup fails, all partially registered
        handlers are rolled back.

        Args:
            plugin_id: ID of the plugin to enable.

        Returns:
            The plugin's metadata with state set to
            :attr:`PluginState.ACTIVE`.

        Raises:
            ValueError: If the plugin is not loaded.
            AttributeError: If the module lacks a ``setup`` function.
            Exception: Re-raises any error from module import or setup,
                after cleaning up partial registrations.
        """
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
            raise AttributeError(f"Plugin module {module_path!r} must expose a setup(plg) function")

        self._register_config_provider_from_module(plugin_id, module)
        plg = self._build_plg(plugin_id)
        try:
            await self._invoke(module.setup, plg)
            await self._invoke_hook(module, "on_enable", plg)
        except Exception:
            # Clean up any handlers registered by setup() so no ghost handlers
            # remain in the EventBus or CommandRegistry when enable fails.
            logger.exception("Error enabling plugin %s; reverting handler registrations", plugin_id)
            self._command_registry.unregister_by_owner(plugin_id)
            self._event_bus.off_all(plugin_id)
            self._keyword_registry.unregister_by_owner(plugin_id)
            self._unregister_routes_by_owner(plugin_id)
            if self._tool_registry is not None:
                self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
            raise

        name, version, description, author, role = self._resolve_identity_fields(
            plugin_id,
            module,
            declared_metadata=self._declared_metadata.get(plugin_id),
        )
        meta.name = name
        meta.version = version
        meta.description = description
        meta.author = author
        meta.role = role
        meta.state = PluginState.ACTIVE
        meta.commands = list(plg._registered_commands)
        meta.event_types = list(plg._registered_events)
        meta.keywords = list(plg._registered_keywords)
        meta.routes = list(plg._registered_routes)
        meta.data_dir = str(plg.data_dir)
        self._plugin_objects[plugin_id] = plg
        self._modules[plugin_id] = module
        logger.info("Enabled plugin %s", plugin_id)
        return meta

    def load_plugins_from_dir(self, directory: Path | str, *, prefix: str = "") -> list[PluginMeta]:
        """Load all plugins discovered in a directory.

        Scans *directory* for ``.py`` files and sub-packages (directories
        containing ``__init__.py``). Entries starting with ``_`` are
        skipped. Already-loaded plugins are skipped with a debug log.

        Args:
            directory: Path to scan for plugin modules.
            prefix: Module prefix used when constructing dotted import
                paths. If empty, defaults to the directory name.

        Returns:
            A list of metadata for each plugin that was successfully
            loaded.

        Raises:
            NotADirectoryError: If *directory* does not exist.
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
                continue

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
        """Synchronously load plugins from a metadata-driven directory.

        This is a blocking wrapper around
        :meth:`load_plugins_from_metadata_dir_async`.

        Args:
            directory: Path containing plugin sub-directories, each with
                a ``metadata.json`` file.

        Returns:
            A list of metadata for each successfully loaded plugin.

        Raises:
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.load_plugins_from_metadata_dir_async(directory))

    async def load_plugins_from_metadata_dir_async(self, directory: Path | str) -> list[PluginMeta]:
        """Asynchronously load plugins from a metadata-driven directory.

        Each sub-directory must contain a ``metadata.json`` with at
        minimum ``id`` and ``entry`` fields. Plugins are topologically
        sorted by declared dependencies before loading.

        Args:
            directory: Path containing plugin sub-directories, each with
                a ``metadata.json`` file.

        Returns:
            A list of metadata for each successfully loaded plugin.

        Raises:
            NotADirectoryError: If *directory* does not exist.
        """
        return await self._load_from_metadata_dir_async(Path(directory), is_builtin=False)

    async def load_all_async(self, user_dir: Path | str | None = None) -> list[PluginMeta]:
        """Asynchronously load built-in and user plugins.

        Loads built-in plugins first from the repository's
        ``builtin_plugins`` directory, then user plugins from
        *user_dir* (defaults to ``<data_dir>/plugins``).

        Args:
            user_dir: Override path for user plugins. When *None*,
                ``<root_data_dir>/plugins`` is used.

        Returns:
            Combined list of metadata from all loaded plugins.
        """
        results: list[PluginMeta] = []
        for candidate in self._discover_plugin_candidates(user_dir=user_dir):
            try:
                if not candidate.is_builtin:
                    installed_dependencies = await sync_plugin_python_dependencies(
                        candidate.metadata["id"],
                        candidate.plugin_dir,
                    )
                    if installed_dependencies:
                        logger.info(
                            "Installed Python dependencies for plugin %s: %s",
                            candidate.metadata["id"],
                            ", ".join(installed_dependencies),
                        )
                meta = await self.load_plugin_async(
                    candidate.metadata["id"],
                    candidate.module_path,
                    declared_metadata=candidate.metadata,
                )
                permissions = candidate.metadata.get("permissions", [])
                self._validate_permissions(candidate.metadata["id"], permissions, meta)
                if candidate.metadata.get("default_enabled") is False:
                    meta = await self.disable_plugin_async(candidate.metadata["id"])
                results.append(meta)
                logger.info(
                    "Loaded %s plugin %s (module=%s)",
                    "builtin" if candidate.is_builtin else "user",
                    candidate.metadata["id"],
                    candidate.module_path,
                )
            except Exception:
                logger.exception(
                    "Failed to load plugin %s from %s",
                    candidate.metadata["id"],
                    candidate.plugin_dir,
                )
        return results

    async def _load_from_metadata_dir_async(
        self, directory: Path, *, is_builtin: bool
    ) -> list[PluginMeta]:
        if not directory.is_dir():
            raise NotADirectoryError(f"Plugin directory not found: {directory}")

        if not is_builtin:
            _ensure_user_plugin_package_on_path(directory)

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

            if not is_builtin:
                try:
                    installed_dependencies = await sync_plugin_python_dependencies(
                        metadata["id"],
                        plugin_dir,
                    )
                except PluginDependencyError:
                    logger.exception(
                        "Failed to install Python dependencies for plugin %s from %s",
                        metadata["id"],
                        plugin_dir,
                    )
                    continue
                if installed_dependencies:
                    logger.info(
                        "Installed Python dependencies for plugin %s: %s",
                        metadata["id"],
                        ", ".join(installed_dependencies),
                    )

            if metadata["id"] in self._plugins:
                logger.debug("Plugin %r already loaded, skipping", metadata["id"])
                continue

            candidates.append((plugin_dir, metadata))

        sorted_candidates = _topo_sort(candidates)

        batch_ids = {m["id"] for _, m in sorted_candidates}
        already_loaded = set(self._plugins.keys())
        for _, metadata in sorted_candidates:
            dependency_ids = [
                *metadata.get("required_dependencies", []),
                *metadata.get("optional_dependencies", []),
                *metadata.get("dependencies", []),
            ]
            for dep in dependency_ids:
                if dep not in batch_ids and dep not in already_loaded:
                    logger.warning(
                        "Plugin %r declares dependency on %r which is not available",
                        metadata["id"],
                        dep,
                    )

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
                module_path = self._module_path_for_candidate(
                    directory=directory,
                    plugin_dir=plugin_dir,
                    metadata=metadata,
                    is_builtin=False,
                )

            try:
                meta = await self.load_plugin_async(
                    plugin_id,
                    module_path,
                    declared_metadata=metadata,
                )
                self._validate_permissions(plugin_id, permissions, meta)
                if metadata.get("default_enabled") is False:
                    meta = await self.disable_plugin_async(plugin_id)
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

    def _discover_plugin_candidates(
        self,
        *,
        user_dir: Path | str | None,
    ) -> list[PluginDiscoveryCandidate]:
        candidates: list[PluginDiscoveryCandidate] = []

        if _BUILTIN_PLUGINS_DIR.is_dir():
            candidates.extend(self._discover_metadata_dir(_BUILTIN_PLUGINS_DIR, is_builtin=True))
        else:
            logger.debug("No built-in plugins directory at %s", _BUILTIN_PLUGINS_DIR)

        user_path = Path(user_dir) if user_dir is not None else self._root_data_dir / "plugins"
        if user_path.is_dir():
            candidates.extend(self._discover_metadata_dir(user_path, is_builtin=False))

        return candidates

    def _discover_metadata_dir(
        self,
        directory: Path,
        *,
        is_builtin: bool,
    ) -> list[PluginDiscoveryCandidate]:
        if not directory.is_dir():
            raise NotADirectoryError(f"Plugin directory not found: {directory}")

        if not is_builtin:
            _ensure_user_plugin_package_on_path(directory)

        raw_candidates: list[tuple[Path, dict[str, Any]]] = []
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

            raw_candidates.append((plugin_dir, metadata))

        sorted_candidates = _topo_sort(raw_candidates)

        batch_ids = {m["id"] for _, m in sorted_candidates}
        already_loaded = set(self._plugins.keys())
        for _, metadata in sorted_candidates:
            dependency_ids = [
                *metadata.get("required_dependencies", []),
                *metadata.get("optional_dependencies", []),
                *metadata.get("dependencies", []),
            ]
            for dep in dependency_ids:
                if dep not in batch_ids and dep not in already_loaded:
                    logger.warning(
                        "Plugin %r declares dependency on %r which is not available",
                        metadata["id"],
                        dep,
                    )

        return [
            PluginDiscoveryCandidate(
                plugin_dir=plugin_dir,
                metadata=metadata,
                module_path=self._module_path_for_candidate(
                    directory=directory,
                    plugin_dir=plugin_dir,
                    metadata=metadata,
                    is_builtin=is_builtin,
                ),
                is_builtin=is_builtin,
            )
            for plugin_dir, metadata in sorted_candidates
        ]

    @staticmethod
    def _module_path_for_candidate(
        *,
        directory: Path,
        plugin_dir: Path,
        metadata: dict[str, Any],
        is_builtin: bool,
    ) -> str:
        entry_file = metadata["entry"]
        if is_builtin:
            pkg = f"shinbot.builtin_plugins.{plugin_dir.name}"
            return (
                pkg
                if entry_file == "__init__.py"
                else f"{pkg}.{'.'.join(Path(entry_file).with_suffix('').parts)}"
            )

        prefix = directory.name
        entry_parts = Path(entry_file).with_suffix("").parts
        if entry_parts and entry_parts[0] == plugin_dir.name:
            _ensure_user_plugin_dir_on_path(plugin_dir)
            import_parts = entry_parts[:-1] if entry_parts[-1] == "__init__" else entry_parts
            return ".".join(import_parts)
        return (
            f"{prefix}.{plugin_dir.name}"
            if entry_file == "__init__.py"
            else f"{prefix}.{plugin_dir.name}.{'.'.join(Path(entry_file).with_suffix('').parts)}"
        )

    def _validate_permissions(
        self, plugin_id: str, declared_permissions: list[str], meta: PluginMeta
    ) -> None:
        cmd_registry = self._command_registry
        registered_commands = cmd_registry._commands.values()

        plugin_perms_used = set()
        for cmd in registered_commands:
            if cmd.owner == plugin_id and cmd.permission:
                plugin_perms_used.add(cmd.permission)

        undeclared = plugin_perms_used - set(declared_permissions)
        if undeclared:
            logger.warning(
                "Plugin %s uses permissions not declared in metadata: %s",
                plugin_id,
                undeclared,
            )

        for perm in declared_permissions:
            if not perm or "." not in perm:
                logger.debug(
                    "Plugin %s declares unusual permission format: %r",
                    plugin_id,
                    perm,
                )

    def reload_plugin(self, plugin_id: str) -> PluginMeta:
        """Synchronously reload a plugin.

        Unloads and immediately re-imports/reloads the plugin module,
        calling ``setup(plg)`` again. The module is kept in
        ``sys.modules`` so ``importlib.reload`` can detect changes.

        This is a blocking wrapper around :meth:`reload_plugin_async`.

        Args:
            plugin_id: ID of the plugin to reload.

        Returns:
            Fresh metadata describing the reloaded plugin.

        Raises:
            ValueError: If the plugin is not loaded.
            RuntimeError: If called from within a running event loop.
        """
        return self._run_sync(self.reload_plugin_async(plugin_id))

    async def reload_plugin_async(self, plugin_id: str) -> PluginMeta:
        """Asynchronously reload a plugin.

        Unloads the plugin's runtime registrations (without removing
        the module from ``sys.modules``), reloads the module, and
        calls ``setup(plg)`` again. Declared metadata is preserved
        across the reload.

        Args:
            plugin_id: ID of the plugin to reload.

        Returns:
            Fresh metadata describing the reloaded plugin.

        Raises:
            ValueError: If the plugin is not loaded.
            Exception: Re-raises any error from module reload or setup,
                after cleaning up partial registrations.
        """
        meta = self._plugins.get(plugin_id)
        if meta is None:
            raise ValueError(f"Plugin {plugin_id!r} is not loaded")

        module_path = meta.module_path
        declared_metadata = self._declared_metadata.get(plugin_id)
        await self.unload_plugin_async(
            plugin_id,
            remove_module=False,
            remove_declared_metadata=False,
        )

        existing = sys.modules.get(module_path)
        if existing is not None and getattr(existing, "__spec__", None) is not None:
            module = importlib.reload(existing)
        elif existing is not None:
            module = existing
        else:
            module = importlib.import_module(module_path)

        logger.info("Reloading plugin %s", plugin_id)
        self._register_config_provider_from_module(plugin_id, module)

        plg = self._build_plg(plugin_id)
        try:
            await self._invoke(module.setup, plg)
            await self._invoke_hook(module, "on_enable", plg)
        except Exception:
            logger.exception(
                "Error during reload of plugin %s; reverting handler registrations", plugin_id
            )
            self._command_registry.unregister_by_owner(plugin_id)
            self._event_bus.off_all(plugin_id)
            self._keyword_registry.unregister_by_owner(plugin_id)
            self._unregister_routes_by_owner(plugin_id)
            if self._tool_registry is not None:
                self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
            raise

        new_meta = self._build_plugin_meta(
            plugin_id,
            module_path,
            module,
            plg,
            declared_metadata=declared_metadata,
        )
        self._plugins[plugin_id] = new_meta
        self._plugin_objects[plugin_id] = plg
        self._modules[plugin_id] = module
        return new_meta

    def _build_plugin_meta(
        self,
        plugin_id: str,
        module_path: str,
        module: Any,
        plg: Plugin,
        *,
        declared_metadata: dict[str, Any] | None = None,
    ) -> PluginMeta:
        name, version, description, author, role = self._resolve_identity_fields(
            plugin_id,
            module,
            declared_metadata=declared_metadata,
        )
        return PluginMeta(
            id=plugin_id,
            name=name,
            version=version,
            description=description,
            author=author,
            role=role,
            state=PluginState.ACTIVE,
            module_path=module_path,
            commands=list(plg._registered_commands),
            event_types=list(plg._registered_events),
            keywords=list(plg._registered_keywords),
            routes=list(plg._registered_routes),
            data_dir=str(plg.data_dir),
        )

    def _register_config_provider_from_module(self, plugin_id: str, module: Any) -> None:
        module_file = getattr(module, "__file__", None)
        if not module_file:
            return
        schema_path = Path(module_file).resolve().parent / "config.schema.toml"
        if not schema_path.exists():
            return
        try:
            provider = load_provider_schema_from_module(module)
        except ConfigProviderLoadError:
            logger.exception("Invalid config provider schema for plugin %s", plugin_id)
            return
        self.config_provider_registry.upsert(provider)

    def _resolve_identity_fields(
        self,
        plugin_id: str,
        module: Any,
        *,
        declared_metadata: dict[str, Any] | None = None,
    ) -> tuple[str, str, str, str, PluginRole]:
        if declared_metadata is None:
            logger.warning("Plugin %r loaded without metadata.json; using defaults", plugin_id)
            declared_metadata = {}
        return (
            declared_metadata.get("name", plugin_id),
            declared_metadata.get("version", "0.0.0"),
            declared_metadata.get("description", ""),
            declared_metadata.get("author", ""),
            self._normalize_role(
                declared_metadata.get("role", PluginRole.LOGIC.value),
                default=PluginRole.LOGIC,
            ),
        )

    def _normalize_role(self, value: Any, *, default: PluginRole = PluginRole.LOGIC) -> PluginRole:
        if isinstance(value, PluginRole):
            return value
        if isinstance(value, str):
            try:
                return PluginRole(value.strip().lower())
            except ValueError:
                return default
        return default

    async def _deactivate_plugin_runtime(
        self,
        plugin_id: str,
        meta: PluginMeta,
        *,
        remove_module: bool,
    ) -> tuple[int, int]:
        module = self._modules.get(plugin_id)
        plg = self._plugin_objects.get(plugin_id)

        try:
            await self._invoke_hook(module, "on_disable", plg)
        except Exception:
            logger.exception("Error in on_disable() for plugin %s", plugin_id)

        cmd_count = self._command_registry.unregister_by_owner(plugin_id)
        evt_count = self._event_bus.off_all(plugin_id)
        self._keyword_registry.unregister_by_owner(plugin_id)
        self._unregister_routes_by_owner(plugin_id)
        if self._tool_registry is not None:
            self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
        if plg is not None and self._model_runtime is not None:
            for observer in plg._registered_model_observers:
                self._model_runtime.unregister_observer(observer)
        if plg is not None and self._cron_manager is not None:
            self._cron_manager.remove_jobs(plugin_id)

        if module and hasattr(module, "teardown"):
            try:
                await self._invoke(module.teardown)
            except Exception:
                logger.exception("Error in teardown() for plugin %s", plugin_id)

        self._modules.pop(plugin_id, None)
        self._plugin_objects.pop(plugin_id, None)

        if remove_module and meta.module_path in sys.modules:
            del sys.modules[meta.module_path]

        return cmd_count, evt_count

    def _unregister_routes_by_owner(self, plugin_id: str) -> None:
        if self._route_table is not None:
            self._route_table.unregister_by_owner(plugin_id)
        if self._route_targets is not None:
            self._route_targets.unregister_by_owner(plugin_id)

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
            if plugin_dir.name != plugin_id:
                raise ValueError(
                    f"Plugin folder name {plugin_dir.name!r} must match metadata.id {plugin_id!r}"
                )
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

        required_deps = metadata.get("required_dependencies", [])
        if not isinstance(required_deps, list) or not all(
            isinstance(d, str) for d in required_deps
        ):
            raise ValueError("metadata.required_dependencies must be a list of plugin ID strings")
        metadata["required_dependencies"] = required_deps

        optional_deps = metadata.get("optional_dependencies", [])
        if not isinstance(optional_deps, list) or not all(
            isinstance(d, str) for d in optional_deps
        ):
            raise ValueError("metadata.optional_dependencies must be a list of plugin ID strings")
        metadata["optional_dependencies"] = optional_deps

        default_enabled = metadata.get("default_enabled", True)
        if not isinstance(default_enabled, bool):
            raise ValueError("metadata.default_enabled must be a boolean")
        metadata["default_enabled"] = default_enabled

        for field in ("name", "version", "author", "description"):
            value = metadata.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"metadata.{field} must be a string")

        name = metadata.get("name", plugin_id)
        version = metadata.get("version", "0.0.0")
        author = metadata.get("author", "")
        description = metadata.get("description", "")
        metadata["name"] = name.strip() or plugin_id
        metadata["version"] = version.strip() or "0.0.0"
        metadata["author"] = author.strip()
        metadata["description"] = description

        role = metadata.get("role", PluginRole.LOGIC.value)
        normalized_role = self._normalize_role(role)
        if isinstance(role, str) and normalized_role.value == role.strip().lower():
            metadata["role"] = normalized_role.value
        elif isinstance(role, PluginRole):
            metadata["role"] = role.value
        else:
            valid_roles = ", ".join(item.value for item in PluginRole)
            raise ValueError(f"metadata.role must be one of: {valid_roles}")

        return metadata

    async def _invoke_hook(self, module: Any, hook_name: str, plg: Plugin | None) -> None:
        if module is None or not hasattr(module, hook_name):
            return
        hook = getattr(module, hook_name)
        try:
            sig = inspect.signature(hook)
            if len(sig.parameters) == 0:
                await self._invoke(hook)
            else:
                await self._invoke(hook, plg)
        except (TypeError, ValueError):
            await self._invoke(hook, plg)

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
