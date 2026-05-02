"""Plugin lifecycle, discovery, and metadata validation."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.keyword import KeywordRegistry
from shinbot.core.model_runtime import ModelRuntimeObserverRegistry
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.types import PluginMeta, PluginRole, PluginState
from shinbot.core.tools import ToolOwnerType, ToolRegistry
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.core.platform.adapter_manager import AdapterManager

logger = get_logger(__name__)

_VALID_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_")
_BUILTIN_PLUGINS_DIR = Path(__file__).resolve().parents[2] / "builtin_plugins"


def _topo_sort(
    candidates: list[tuple[Path, dict[str, Any]]],
) -> list[tuple[Path, dict[str, Any]]]:
    id_to_item: dict[str, tuple[Path, dict[str, Any]]] = {m["id"]: (d, m) for d, m in candidates}
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


class PluginManager:
    """Manages plugin lifecycle: load, unload, reload."""

    def __init__(
        self,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        keyword_registry: KeywordRegistry | None = None,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
        model_runtime: ModelRuntimeObserverRegistry | None = None,
        database: Any | None = None,
    ):
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._keyword_registry = keyword_registry or KeywordRegistry()
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._model_runtime = model_runtime
        self._database = database
        self._plugins: dict[str, PluginMeta] = {}
        self._plugin_objects: dict[str, Plugin] = {}
        self._modules: dict[str, Any] = {}
        self._declared_metadata: dict[str, dict[str, Any]] = {}

        self._root_data_dir = Path(data_dir) if data_dir is not None else Path("data")
        self._plugin_data_root = self._root_data_dir / "plugin_data"
        self._plugin_data_root.mkdir(parents=True, exist_ok=True)

    def _build_plg(self, plugin_id: str) -> Plugin:
        return Plugin(
            plugin_id,
            self._command_registry,
            self._event_bus,
            data_dir=self._build_plugin_data_dir(plugin_id),
            keyword_registry=self._keyword_registry,
            adapter_manager=self._adapter_manager,
            tool_registry=self._tool_registry,
            model_runtime=self._model_runtime,
            database=self._database,
        )

    @property
    def all_plugins(self) -> list[PluginMeta]:
        return list(self._plugins.values())

    def get_plugin(self, plugin_id: str) -> PluginMeta | None:
        return self._plugins.get(plugin_id)

    def load_plugin(self, plugin_id: str, module_path: str) -> PluginMeta:
        return self._run_sync(self.load_plugin_async(plugin_id, module_path))

    async def load_plugin_async(
        self,
        plugin_id: str,
        module_path: str,
        *,
        declared_metadata: dict[str, Any] | None = None,
    ) -> PluginMeta:
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

        plg = self._build_plg(plugin_id)

        try:
            await self._invoke(module.setup, plg)
            await self._invoke_hook(module, "on_enable", plg)
        except Exception:
            logger.exception("Error loading plugin %s", plugin_id)
            self._command_registry.unregister_by_owner(plugin_id)
            self._event_bus.off_all(plugin_id)
            self._keyword_registry.unregister_by_owner(plugin_id)
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
        return self._run_sync(self.unload_plugin_async(plugin_id))

    async def unload_plugin_async(
        self,
        plugin_id: str,
        *,
        remove_module: bool = True,
        remove_declared_metadata: bool = True,
    ) -> bool:
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
        logger.info(
            "Disabled plugin %s (removed %d commands, %d event handlers)",
            plugin_id,
            cmd_count,
            evt_count,
        )
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
            raise AttributeError(f"Plugin module {module_path!r} must expose a setup(plg) function")

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
        meta.data_dir = str(plg.data_dir)
        self._plugin_objects[plugin_id] = plg
        self._modules[plugin_id] = module
        logger.info("Enabled plugin %s", plugin_id)
        return meta

    def load_plugins_from_dir(self, directory: Path | str, *, prefix: str = "") -> list[PluginMeta]:
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
        return self._run_sync(self.load_plugins_from_metadata_dir_async(directory))

    async def load_plugins_from_metadata_dir_async(self, directory: Path | str) -> list[PluginMeta]:
        return await self._load_from_metadata_dir_async(Path(directory), is_builtin=False)

    async def load_all_async(self, user_dir: Path | str | None = None) -> list[PluginMeta]:
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
        if not directory.is_dir():
            raise NotADirectoryError(f"Plugin directory not found: {directory}")

        if not is_builtin:
            parent = str(directory.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)

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

        sorted_candidates = _topo_sort(candidates)

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
                meta = await self.load_plugin_async(
                    plugin_id,
                    module_path,
                    declared_metadata=metadata,
                )
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
        return self._run_sync(self.reload_plugin_async(plugin_id))

    async def reload_plugin_async(self, plugin_id: str) -> PluginMeta:
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
            data_dir=str(plg.data_dir),
        )

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
        if self._tool_registry is not None:
            self._tool_registry.unregister_owner(ToolOwnerType.PLUGIN, plugin_id)
        if plg is not None and self._model_runtime is not None:
            for observer in plg._registered_model_observers:
                self._model_runtime.unregister_observer(observer)

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
