"""Plugin management router: /api/v1/plugins"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, ok
from shinbot.core.plugins.config import (
    normalize_plugin_config,
    plugin_config_schema,
    plugin_config_store,
    plugin_locales,
    plugin_saved_config,
    plugin_module,
    request_locales,
    resolve_translations,
    translate_plugin_schema,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    dependencies=AuthRequired,
)


def _plugin_dict(
    bot: Any,
    p: Any,
    boot: Any | None = None,
    *,
    translations: dict[str, str] | None = None,
) -> dict:
    module = plugin_module(bot.plugin_manager, p.id)
    cfg_schema = None
    try:
        cfg_schema = plugin_config_schema(bot.plugin_manager, p.id)
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", p.id)
    status = "enabled" if p.state.value in ("active", "loaded", "running") else "disabled"
    resolved_translations = translations or {}

    metadata: dict[str, Any] = {}
    if cfg_schema is not None:
        metadata["config_schema"] = translate_plugin_schema(cfg_schema, resolved_translations)
    if boot is not None:
        metadata["config"] = plugin_saved_config(boot, p.id)
    if module is not None and hasattr(module, "__plugin_adapter_platform__"):
        metadata["adapter_platform"] = module.__plugin_adapter_platform__

    return {
        "id": p.id,
        "name": resolved_translations.get("meta.name", p.name),
        "version": p.version,
        "description": resolved_translations.get("meta.description", p.description),
        "author": p.author,
        "role": p.role.value,
        "status": status,
        "state": p.state.value,
        "commands": p.commands,
        "event_types": p.event_types,
        "data_dir": p.data_dir,
        "metadata": metadata,
    }


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_plugins(request: Request, bot=BotDep, boot=BootDep):
    """List all loaded plugins with their metadata and config schema."""
    requested = request_locales(request.headers.get("accept-language", ""))
    return ok(
        [
            _plugin_dict(
                bot,
                p,
                boot,
                translations=resolve_translations(
                    plugin_locales(bot.plugin_manager, p.id), requested
                ),
            )
            for p in bot.plugin_manager.all_plugins
        ]
    )


@router.get("/{plugin_id}/schema")
async def get_plugin_schema(plugin_id: str, request: Request, bot=BotDep):
    """Get config schema for a non-adapter plugin that declares __plugin_config_class__."""
    plugin = bot.plugin_manager.get_plugin(plugin_id)
    if plugin is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": f"Plugin {plugin_id!r} not found"},
        )

    if plugin.role.value == "adapter":
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose plugin-level configuration",
            },
        )

    try:
        schema = plugin_config_schema(bot.plugin_manager, plugin_id)
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", plugin_id)
        schema = None
    if schema is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose a config schema",
            },
        )
    translations = resolve_translations(
        plugin_locales(bot.plugin_manager, plugin_id),
        request_locales(request.headers.get("accept-language", "")),
    )
    return ok(translate_plugin_schema(schema, translations))


async def _rescan_plugins(bot: Any, boot: Any):
    """Rescan data/plugins/ for new plugins and load them (hot-add)."""
    plugins_dir = (Path(boot.data_dir) / "plugins").resolve()
    if not plugins_dir.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_RESCAN_FAILED,
                "message": f"Plugins directory not found: {plugins_dir}",
            },
        )
    try:
        loaded = await bot.plugin_manager.load_plugins_from_metadata_dir_async(plugins_dir)
        return ok({"loaded_count": len(loaded), "plugins": [_plugin_dict(bot, p) for p in loaded]})
    except Exception as e:
        logger.exception("Plugin rescan failed")
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RESCAN_FAILED, "message": str(e)},
        ) from e


@router.post("/reload")
async def reload_plugins(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.post("/rescan")
async def rescan_plugins(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.patch("/{plugin_id}/config")
async def update_plugin_config(plugin_id: str, config: dict[str, Any], bot=BotDep, boot=BootDep):
    """Persist plugin configuration for a specific plugin by ID."""
    plugin = bot.plugin_manager.get_plugin(plugin_id)
    if plugin is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": f"Plugin {plugin_id!r} not found"},
        )

    if plugin.role.value == "adapter":
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose plugin-level configuration",
            },
        )

    try:
        normalized_config = normalize_plugin_config(bot.plugin_manager, plugin_id, config)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.INVALID_ACTION,
                "message": exc.errors()[0].get("msg", "Invalid plugin configuration"),
            },
        ) from exc

    store = plugin_config_store(boot)
    store[plugin_id] = normalized_config

    if not boot.save_config():
        raise HTTPException(
            status_code=500,
            detail={
                "code": EC.CONFIG_WRITE_FAILED,
                "message": f"Failed to persist configuration for plugin {plugin_id!r}",
            },
        )

    return ok(_plugin_dict(bot, plugin, boot))


@router.post("/{plugin_id}/disable")
async def disable_plugin(plugin_id: str, bot=BotDep):
    """Disable a specific plugin while keeping its metadata visible to the UI."""
    try:
        meta = await bot.plugin_manager.disable_plugin_async(plugin_id)
        return ok(_plugin_dict(bot, meta))
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": str(e)},
        ) from e
    except Exception as e:
        logger.exception("Disable failed for plugin %s", plugin_id)
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RELOAD_FAILED, "message": str(e)},
        ) from e


@router.post("/{plugin_id}/enable")
async def enable_plugin(plugin_id: str, bot=BotDep):
    """Enable a previously disabled plugin."""
    try:
        meta = await bot.plugin_manager.enable_plugin_async(plugin_id)
        return ok(_plugin_dict(bot, meta))
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": str(e)},
        ) from e
    except Exception as e:
        logger.exception("Enable failed for plugin %s", plugin_id)
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RELOAD_FAILED, "message": str(e)},
        ) from e
