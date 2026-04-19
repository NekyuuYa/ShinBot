"""Plugin management router: /api/v1/plugins"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import ok
from shinbot.core.plugin_admin import (
    PluginAdminError,
    disable_plugin_or_raise,
    enable_plugin_or_raise,
    get_plugin_schema_or_raise,
    plugin_dict,
    plugin_translations,
    rescan_plugins,
    update_plugin_config_or_raise,
)
from shinbot.core.plugins.config import request_locales

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    dependencies=AuthRequired,
)

def _raise_admin_http_error(exc: PluginAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_plugins(request: Request, bot=BotDep, boot=BootDep):
    """List all loaded plugins with their metadata and config schema."""
    requested = request_locales(request.headers.get("accept-language", ""))
    return ok(
        [
            plugin_dict(
                bot,
                p,
                boot,
                translations=plugin_translations(bot, p.id, requested),
            )
            for p in bot.plugin_manager.all_plugins
        ]
    )


@router.get("/{plugin_id}/schema")
async def get_plugin_schema(plugin_id: str, request: Request, bot=BotDep):
    """Get config schema for a non-adapter plugin that declares __plugin_config_class__."""
    try:
        return ok(
            get_plugin_schema_or_raise(
                bot,
                plugin_id,
                plugin_translations(
                    bot,
                    plugin_id,
                    request_locales(request.headers.get("accept-language", "")),
                ),
            )
        )
    except PluginAdminError as exc:
        _raise_admin_http_error(exc)


async def _rescan_plugins(bot: Any, boot: Any):
    """Rescan data/plugins/ for new plugins and load them (hot-add)."""
    try:
        loaded = await rescan_plugins(bot, boot)
        return ok({"loaded_count": len(loaded), "plugins": loaded})
    except PluginAdminError as exc:
        _raise_admin_http_error(exc)


@router.post("/reload")
async def reload_plugins(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.post("/rescan")
async def rescan_plugins_route(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.patch("/{plugin_id}/config")
async def update_plugin_config(plugin_id: str, config: dict[str, Any], bot=BotDep, boot=BootDep):
    """Persist plugin configuration for a specific plugin by ID."""
    try:
        plugin = update_plugin_config_or_raise(bot, boot, plugin_id, config)
    except PluginAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(plugin_dict(bot, plugin, boot))


@router.post("/{plugin_id}/disable")
async def disable_plugin(plugin_id: str, bot=BotDep):
    """Disable a specific plugin while keeping its metadata visible to the UI."""
    try:
        meta = await disable_plugin_or_raise(bot, plugin_id)
    except PluginAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(plugin_dict(bot, meta))


@router.post("/{plugin_id}/enable")
async def enable_plugin(plugin_id: str, bot=BotDep):
    """Enable a previously disabled plugin."""
    try:
        meta = await enable_plugin_or_raise(bot, plugin_id)
    except PluginAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(plugin_dict(bot, meta))
