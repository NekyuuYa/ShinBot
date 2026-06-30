"""Plugin marketplace router: /api/v1/plugin-marketplace."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from shinbot.admin.plugin_marketplace import (
    OFFICIAL_MARKETPLACE_SOURCE_ID,
    PluginMarketplaceError,
    build_plugin_marketplace_service,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/plugin-marketplace",
    tags=["plugin-marketplace"],
    dependencies=AuthRequired,
)


class PluginMarketplaceSourceRequest(BaseModel):
    """Marketplace source selector request."""

    model_config = ConfigDict(extra="forbid")

    source: str = OFFICIAL_MARKETPLACE_SOURCE_ID
    refresh: bool = False


class PluginMarketplaceInstallRequest(PluginMarketplaceSourceRequest):
    """Marketplace install request."""

    enable_after_install: bool = True
    allow_overwrite: bool = False


def _raise_marketplace_http_error(exc: PluginMarketplaceError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("/sources", response_model=Envelope[dict[str, Any]])
async def list_plugin_marketplace_sources(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List configured plugin marketplace sources."""
    service = build_plugin_marketplace_service(bot, boot)
    return ok(service.list_sources())


@router.get("", response_model=Envelope[dict[str, Any]])
async def list_plugin_marketplace(
    source: str = Query(default=OFFICIAL_MARKETPLACE_SOURCE_ID),
    refresh: bool = Query(default=False),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """List plugins from a marketplace source."""
    service = build_plugin_marketplace_service(bot, boot)
    try:
        return ok(await service.list_plugins(source, refresh=refresh))
    except PluginMarketplaceError as exc:
        _raise_marketplace_http_error(exc)


@router.get("/{plugin_id}", response_model=Envelope[dict[str, Any]])
async def get_plugin_marketplace_item(
    plugin_id: str,
    source: str = Query(default=OFFICIAL_MARKETPLACE_SOURCE_ID),
    refresh: bool = Query(default=False),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Get one plugin from a marketplace source."""
    service = build_plugin_marketplace_service(bot, boot)
    try:
        return ok(await service.get_plugin(source, plugin_id, refresh=refresh))
    except PluginMarketplaceError as exc:
        _raise_marketplace_http_error(exc)


@router.post("/{plugin_id}/preview", response_model=Envelope[dict[str, Any]])
async def preview_plugin_marketplace_item(
    plugin_id: str,
    payload: PluginMarketplaceSourceRequest | None = None,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Preview installing one marketplace plugin."""
    service = build_plugin_marketplace_service(bot, boot)
    source = payload.source if payload is not None else OFFICIAL_MARKETPLACE_SOURCE_ID
    refresh = payload.refresh if payload is not None else False
    try:
        return ok(await service.preview_plugin(source, plugin_id, refresh=refresh))
    except PluginMarketplaceError as exc:
        _raise_marketplace_http_error(exc)


@router.post("/{plugin_id}/install", response_model=Envelope[dict[str, Any]])
async def install_plugin_marketplace_item(
    plugin_id: str,
    payload: PluginMarketplaceInstallRequest | None = None,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Install one marketplace plugin."""
    service = build_plugin_marketplace_service(bot, boot)
    request_payload = payload or PluginMarketplaceInstallRequest()
    try:
        return ok(
            await service.install_plugin(
                request_payload.source,
                plugin_id,
                enable_after_install=request_payload.enable_after_install,
                allow_overwrite=request_payload.allow_overwrite,
                refresh=request_payload.refresh,
            )
        )
    except PluginMarketplaceError as exc:
        _raise_marketplace_http_error(exc)
