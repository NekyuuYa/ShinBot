"""WebUI-managed plugin installation router: /api/v1/plugin-installs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict

from shinbot.admin.plugin_install import (
    PLUGIN_INSTALL_MAX_ARCHIVE_BYTES,
    PluginInstallError,
    build_plugin_install_service,
)
from shinbot.admin.plugin_marketplace import (
    PluginMarketplaceError,
    build_plugin_marketplace_service,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/plugin-installs",
    tags=["plugin-installs"],
    dependencies=AuthRequired,
)


class GithubPluginInstallRequest(BaseModel):
    """GitHub plugin install or preview request."""

    model_config = ConfigDict(extra="forbid")

    url: str
    ref: str = "main"
    plugin_path: str = ""
    enable_after_install: bool = True
    allow_overwrite: bool = False
    installer_type: str = "shinbot"


def _raise_install_http_error(exc: PluginInstallError | PluginMarketplaceError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


async def _read_limited_archive_body(request: Request) -> bytes:
    """Read a raw zip request body with a hard size limit."""
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_size = int(content_length)
        except ValueError:
            declared_size = PLUGIN_INSTALL_MAX_ARCHIVE_BYTES + 1
        if declared_size > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginInstallError(
                status_code=413,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive is too large",
            )

    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginInstallError(
                status_code=413,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive is too large",
            )
    return bytes(body)


@router.get("", response_model=Envelope[dict[str, Any]])
async def list_plugin_install_sources(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List WebUI-managed plugin source records."""
    service = build_plugin_install_service(bot, boot)
    try:
        payload = service.list_sources()
        payload.update(build_plugin_marketplace_service(bot, boot).list_installers())
        return ok(payload)
    except (PluginInstallError, PluginMarketplaceError) as exc:
        _raise_install_http_error(exc)


@router.get("/tasks/{task_id}", response_model=Envelope[dict[str, Any]])
async def get_plugin_install_task(task_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Get an in-memory plugin install task status."""
    service = build_plugin_install_service(bot, boot)
    task = service.get_task(task_id)
    if task is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "PLUGIN_INSTALL_TASK_NOT_FOUND", "message": "Plugin install task not found"},
        )
    return ok(task)


@router.post("/github/preview", response_model=Envelope[dict[str, Any]])
async def preview_github_plugin_install(payload: GithubPluginInstallRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Preview a GitHub plugin archive without installing it."""
    try:
        if payload.installer_type != "shinbot":
            return ok(
                await build_plugin_marketplace_service(bot, boot).preview_custom_github(
                    installer_type=payload.installer_type,
                    repository_url=payload.url,
                    ref=payload.ref,
                    plugin_path=payload.plugin_path,
                )
            )
        service = build_plugin_install_service(bot, boot)
        return ok(
            await service.preview_github(
                payload.url,
                payload.ref,
                plugin_path=payload.plugin_path,
            )
        )
    except (PluginInstallError, PluginMarketplaceError) as exc:
        _raise_install_http_error(exc)


@router.post("/github", response_model=Envelope[dict[str, Any]])
async def install_github_plugin(payload: GithubPluginInstallRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Install a plugin from a GitHub repository archive."""
    try:
        if payload.installer_type != "shinbot":
            return ok(
                await build_plugin_marketplace_service(bot, boot).install_custom_github(
                    installer_type=payload.installer_type,
                    repository_url=payload.url,
                    ref=payload.ref,
                    plugin_path=payload.plugin_path,
                    enable_after_install=payload.enable_after_install,
                    allow_overwrite=payload.allow_overwrite,
                )
            )
        service = build_plugin_install_service(bot, boot)
        return ok(
            await service.install_github(
                payload.url,
                payload.ref,
                plugin_path=payload.plugin_path,
                enable_after_install=payload.enable_after_install,
                allow_overwrite=payload.allow_overwrite,
            )
        )
    except (PluginInstallError, PluginMarketplaceError) as exc:
        _raise_install_http_error(exc)


@router.post("/archive/preview", response_model=Envelope[dict[str, Any]])
async def preview_archive_plugin_install(
    request: Request,
    filename: str = Query(default=""),
    installer_type: str = Query(default="shinbot"),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Preview a raw application/zip plugin archive without installing it."""
    try:
        archive_bytes = await _read_limited_archive_body(request)
        if installer_type != "shinbot":
            return ok(
                await build_plugin_marketplace_service(bot, boot).preview_custom_archive(
                    installer_type=installer_type,
                    archive_bytes=archive_bytes,
                    filename=filename,
                )
            )
        service = build_plugin_install_service(bot, boot)
        return ok(await service.preview_archive(archive_bytes, filename=filename))
    except (PluginInstallError, PluginMarketplaceError) as exc:
        _raise_install_http_error(exc)


@router.post("/archive", response_model=Envelope[dict[str, Any]])
async def install_archive_plugin(
    request: Request,
    enable_after_install: bool = Query(default=True),
    allow_overwrite: bool = Query(default=False),
    filename: str = Query(default=""),
    installer_type: str = Query(default="shinbot"),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Install a plugin from a raw application/zip request body."""
    try:
        archive_bytes = await _read_limited_archive_body(request)
        if installer_type != "shinbot":
            return ok(
                await build_plugin_marketplace_service(bot, boot).install_custom_archive(
                    installer_type=installer_type,
                    archive_bytes=archive_bytes,
                    filename=filename,
                    enable_after_install=enable_after_install,
                    allow_overwrite=allow_overwrite,
                )
            )
        service = build_plugin_install_service(bot, boot)
        return ok(
            await service.install_archive(
                archive_bytes,
                filename=filename,
                enable_after_install=enable_after_install,
                allow_overwrite=allow_overwrite,
            )
        )
    except (PluginInstallError, PluginMarketplaceError) as exc:
        _raise_install_http_error(exc)


@router.post("/{plugin_id}/update", response_model=Envelope[dict[str, Any]])
async def update_webui_plugin(
    plugin_id: str,
    enable_after_install: bool = Query(default=True),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Update a WebUI-managed GitHub plugin."""
    service = build_plugin_install_service(bot, boot)
    try:
        return ok(await service.update_plugin(plugin_id, enable_after_install=enable_after_install))
    except PluginInstallError as exc:
        _raise_install_http_error(exc)


@router.delete("/{plugin_id}", response_model=Envelope[dict[str, Any]])
async def uninstall_webui_plugin(plugin_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Uninstall a WebUI-managed plugin while preserving plugin data."""
    service = build_plugin_install_service(bot, boot)
    try:
        return ok(await service.uninstall_plugin(plugin_id))
    except PluginInstallError as exc:
        _raise_install_http_error(exc)
