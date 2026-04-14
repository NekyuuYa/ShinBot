"""Plugin management router: /api/v1/plugins"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, ok

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    dependencies=AuthRequired,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _adapter_config_schema(bot: Any, plugin_id: str) -> dict[str, Any] | None:
    module = bot.plugin_manager._modules.get(plugin_id)
    if module is None:
        return None

    cfg_cls = getattr(module, "__plugin_config_class__", None)
    if cfg_cls is None or not hasattr(cfg_cls, "model_json_schema"):
        return None

    try:
        return cfg_cls.model_json_schema()
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", plugin_id)
        return None


def _plugin_dict(bot: Any, p: Any) -> dict:
    module = bot.plugin_manager._modules.get(p.id)
    cfg_schema = _adapter_config_schema(bot, p.id)
    status = "enabled" if p.state.value in ("active", "loaded", "running") else "disabled"

    metadata: dict[str, Any] = {}
    if cfg_schema is not None:
        metadata["config_schema"] = cfg_schema
    if module is not None and hasattr(module, "__plugin_adapter_platform__"):
        metadata["adapter_platform"] = module.__plugin_adapter_platform__

    return {
        "id": p.id,
        "name": p.name,
        "version": p.version,
        "description": p.description,
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
async def list_plugins(bot=BotDep):
    """List all loaded plugins with their metadata and config schema."""
    return ok([_plugin_dict(bot, p) for p in bot.plugin_manager.all_plugins])


@router.get("/{plugin_id}/schema")
async def get_plugin_schema(plugin_id: str, bot=BotDep):
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

    schema = _adapter_config_schema(bot, plugin_id)
    if schema is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose a config schema",
            },
        )
    return ok(schema)


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
async def reload_plugin(plugin_id: str, bot=BotDep):
    """Hot-reload a specific plugin by ID."""
    try:
        meta = await bot.plugin_manager.reload_plugin_async(plugin_id)
        return ok(_plugin_dict(bot, meta))
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": str(e)},
        ) from e
    except Exception as e:
        logger.exception("Hot-reload failed for plugin %s", plugin_id)
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RELOAD_FAILED, "message": str(e)},
        ) from e


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
