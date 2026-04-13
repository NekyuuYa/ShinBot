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


def _plugin_dict(p: Any) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "version": p.version,
        "description": p.description,
        "author": p.author,
        "state": p.state.value,
        "commands": p.commands,
        "event_types": p.event_types,
        "data_dir": p.data_dir,
    }


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_plugins(bot=BotDep):
    """List all loaded plugins with their metadata and config schema."""
    return ok([_plugin_dict(p) for p in bot.plugin_manager.all_plugins])


@router.post("/reload")
async def rescan_plugins(bot=BotDep, boot=BootDep):
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
        return ok({"loaded_count": len(loaded), "plugins": [_plugin_dict(p) for p in loaded]})
    except Exception as e:
        logger.exception("Plugin rescan failed")
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RESCAN_FAILED, "message": str(e)},
        ) from e


@router.patch("/{plugin_id}/config")
async def reload_plugin(plugin_id: str, bot=BotDep):
    """Hot-reload a specific plugin by ID."""
    try:
        meta = await bot.plugin_manager.reload_plugin_async(plugin_id)
        return ok(_plugin_dict(meta))
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
