"""Administrative helpers for plugin management flows."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from shinbot.core.plugins.config import (
    normalize_plugin_config,
    plugin_config_schema,
    plugin_config_store,
    plugin_locales,
    plugin_module,
    plugin_saved_config,
    resolve_translations,
    translate_plugin_schema,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PluginAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def plugin_dict(
    bot: Any,
    plugin_meta: Any,
    boot: Any | None = None,
    *,
    translations: dict[str, str] | None = None,
) -> dict[str, Any]:
    module = plugin_module(bot.plugin_manager, plugin_meta.id)
    cfg_schema = None
    try:
        cfg_schema = plugin_config_schema(bot.plugin_manager, plugin_meta.id)
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", plugin_meta.id)
    status = (
        "enabled"
        if plugin_meta.state.value in ("active", "loaded", "running")
        else "disabled"
    )
    resolved_translations = translations or {}

    metadata: dict[str, Any] = {}
    if cfg_schema is not None:
        metadata["config_schema"] = translate_plugin_schema(cfg_schema, resolved_translations)
    if boot is not None:
        metadata["config"] = plugin_saved_config(boot, plugin_meta.id)
    if module is not None and hasattr(module, "__plugin_adapter_platform__"):
        metadata["adapter_platform"] = module.__plugin_adapter_platform__

    return {
        "id": plugin_meta.id,
        "name": resolved_translations.get("meta.name", plugin_meta.name),
        "version": plugin_meta.version,
        "description": resolved_translations.get("meta.description", plugin_meta.description),
        "author": plugin_meta.author,
        "role": plugin_meta.role.value,
        "status": status,
        "state": plugin_meta.state.value,
        "commands": plugin_meta.commands,
        "event_types": plugin_meta.event_types,
        "data_dir": plugin_meta.data_dir,
        "metadata": metadata,
    }


def get_plugin_or_raise(plugin_manager: Any, plugin_id: str) -> Any:
    plugin = plugin_manager.get_plugin(plugin_id)
    if plugin is None:
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=f"Plugin {plugin_id!r} not found",
        )
    return plugin


def get_plugin_schema_or_raise(bot: Any, plugin_id: str, translations: dict[str, str]) -> dict[str, Any]:
    plugin = get_plugin_or_raise(bot.plugin_manager, plugin_id)
    if plugin.role.value == "adapter":
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=f"Plugin {plugin_id!r} does not expose plugin-level configuration",
        )

    try:
        schema = plugin_config_schema(bot.plugin_manager, plugin_id)
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", plugin_id)
        schema = None
    if schema is None:
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=f"Plugin {plugin_id!r} does not expose a config schema",
        )
    return translate_plugin_schema(schema, translations)


async def rescan_plugins(bot: Any, boot: Any) -> list[dict[str, Any]]:
    plugins_dir = (Path(boot.data_dir) / "plugins").resolve()
    if not plugins_dir.exists():
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_RESCAN_FAILED",
            message=f"Plugins directory not found: {plugins_dir}",
        )
    try:
        loaded = await bot.plugin_manager.load_plugins_from_metadata_dir_async(plugins_dir)
    except Exception as exc:
        logger.exception("Plugin rescan failed")
        raise PluginAdminError(
            status_code=500,
            code="PLUGIN_RESCAN_FAILED",
            message=str(exc),
        ) from exc
    return [plugin_dict(bot, plugin_meta) for plugin_meta in loaded]


def update_plugin_config_or_raise(bot: Any, boot: Any, plugin_id: str, config: dict[str, Any]) -> Any:
    plugin = get_plugin_or_raise(bot.plugin_manager, plugin_id)
    if plugin.role.value == "adapter":
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=f"Plugin {plugin_id!r} does not expose plugin-level configuration",
        )

    try:
        normalized_config = normalize_plugin_config(bot.plugin_manager, plugin_id, config)
    except ValidationError as exc:
        raise PluginAdminError(
            status_code=422,
            code="INVALID_ACTION",
            message=exc.errors()[0].get("msg", "Invalid plugin configuration"),
        ) from exc

    store = plugin_config_store(boot)
    store[plugin_id] = normalized_config

    if not boot.save_config():
        raise PluginAdminError(
            status_code=500,
            code="CONFIG_WRITE_FAILED",
            message=f"Failed to persist configuration for plugin {plugin_id!r}",
        )

    return plugin


async def disable_plugin_or_raise(bot: Any, plugin_id: str) -> Any:
    try:
        return await bot.plugin_manager.disable_plugin_async(plugin_id)
    except ValueError as exc:
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("Disable failed for plugin %s", plugin_id)
        raise PluginAdminError(
            status_code=500,
            code="PLUGIN_RELOAD_FAILED",
            message=str(exc),
        ) from exc


async def enable_plugin_or_raise(bot: Any, plugin_id: str) -> Any:
    try:
        return await bot.plugin_manager.enable_plugin_async(plugin_id)
    except ValueError as exc:
        raise PluginAdminError(
            status_code=404,
            code="PLUGIN_NOT_FOUND",
            message=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("Enable failed for plugin %s", plugin_id)
        raise PluginAdminError(
            status_code=500,
            code="PLUGIN_RELOAD_FAILED",
            message=str(exc),
        ) from exc


def plugin_translations(bot: Any, plugin_id: str, requested_locales: list[str]) -> dict[str, str]:
    return resolve_translations(
        plugin_locales(bot.plugin_manager, plugin_id),
        requested_locales,
    )
